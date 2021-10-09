package rpc_client

import (
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Client struct {
	client            *rpc.Client
	url               string
	healthcheckMethod string

	*sync.Mutex
}

func New(url string, healthcheckMethod string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", url)
	if err != nil {
		logrus.WithError(err).Fatal("can't make rpc_client connection")
	}

	c := &Client{
		client:            rpcClient,
		url:               url,
		healthcheckMethod: healthcheckMethod,
		Mutex:             &sync.Mutex{},
	}
	go c.serveConnection()
	return c
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	c.Lock()
	err := c.client.Call(serviceMethod, args, reply)
	c.Unlock()
	return err
}

func (c *Client) serveConnection() {
	for {
		logrus.Debug("rpc healthcheck called")
		c.Lock()
		err := c.client.Call(c.healthcheckMethod, struct{}{}, &struct{}{})
		if err != nil {
			logrus.WithError(err).Warn("can't do rpc healthcheck call")
			c.client = c.restoreConnection()
		}
		c.Unlock()
		time.Sleep(time.Minute * 2)
	}
}

func (c *Client) restoreConnection() *rpc.Client {
	var i int // i counter just for omit error log when first iteration
	for i++; ; {
		newClient, err := rpc.DialHTTP("tcp", c.url)
		if err == nil {
			logrus.Debug("rpc connection restored")
			return newClient
		}
		if i > 0 {
			logrus.WithError(err).Error("can't restore rpc connection")
		} else {
			logrus.WithError(err).Warn("can't restore rpc connection")
		}
		i++
		time.Sleep(time.Minute)
	}
}
