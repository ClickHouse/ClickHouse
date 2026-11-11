#pragma once

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>


namespace DB
{

struct RabbitMQConfiguration
{
    String host;
    UInt16 port{};
    String username;
    String password;
    String vhost;

    bool secure{};
    String connection_string;
};

class RabbitMQConnection
{
public:
    RabbitMQConnection(const RabbitMQConfiguration & configuration_, LoggerPtr log_);
    ~RabbitMQConnection();

    bool isConnected();

    bool connect();

    bool reconnect();

    void disconnect(bool immediately = false);

    void heartbeat();

    /// The channel is created on the loop thread; the caller must set its callbacks there as well
    /// (inside the same RabbitMQHandler::run()), or the first frames may arrive before they are set.
    ChannelPtr createChannel();

    /// RabbitMQHandler is thread safe. Any public methods can be called concurrently.
    RabbitMQHandler & getHandler() { return event_handler; }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl();

    void connectImpl();

    void disconnectImpl(bool immediately = false);

    /// Poll `done` on the loop thread until it holds or the retries run out, `sleep_ms` apart.
    /// Drives the loop itself when no loop thread does.
    void waitFor(std::function<bool()> done, UInt64 sleep_ms);

    RabbitMQConfiguration configuration;
    LoggerPtr log;

    UVLoop loop;
    /// Preserve order of destruction here:
    /// destruct connection and handler before the loop above.
    RabbitMQHandler event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;

    /// Serializes connection-level operations between callers (streaming task, direct SELECT, shutdown).
    std::mutex mutex;
};

using RabbitMQConnectionPtr = std::unique_ptr<RabbitMQConnection>;

}
