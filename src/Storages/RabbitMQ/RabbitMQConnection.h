#pragma once

#include <Storages/RabbitMQ/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>


namespace DB
{

struct RabbitMQConfiguration
{
    String host;
    UInt16 port;
    String username;
    String password;
    String vhost;
};

class RabbitMQConnection
{
public:
    RabbitMQConnection(const RabbitMQConfiguration & configuration_, Poco::Logger * log_);

    bool isConnected();

    bool connect();

    bool reconnect();

    void disconnect(bool immediately = false);

    void heartbeat();

    bool closed();

    ChannelPtr createChannel();

    /// RabbitMQHandler is thread safe. Any public methods can be called concurrently.
    RabbitMQHandler & getHandler() { return event_handler; }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl() const;

    void connectImpl();

    void disconnectImpl(bool immediately = false);

    RabbitMQConfiguration configuration;
    Poco::Logger * log;

    UVLoop loop;
    RabbitMQHandler event_handler;

    std::unique_ptr<AMQP::TcpConnection> connection;
    std::mutex mutex;
};

using RabbitMQConnectionPtr = std::unique_ptr<RabbitMQConnection>;

}
