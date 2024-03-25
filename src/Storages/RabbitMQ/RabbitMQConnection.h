#pragma once

#include <Storages/UVLoop.h>
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

    bool secure;
    String connection_string;
};

class RabbitMQConnection
{
public:
    RabbitMQConnection(const RabbitMQConfiguration & configuration_, LoggerPtr log_);

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
    LoggerPtr log;

    UVLoop loop;
    /// Preserve order of destruction here:
    /// destruct connection and handler before the loop above.
    RabbitMQHandler event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;

    std::mutex mutex;
};

using RabbitMQConnectionPtr = std::unique_ptr<RabbitMQConnection>;

}
