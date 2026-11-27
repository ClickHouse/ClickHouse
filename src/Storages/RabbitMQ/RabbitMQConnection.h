#pragma once

#include <Common/Logger.h>
#include <Common/SecretString.h>
#include <Storages/RabbitMQ/AMQPClient.h>

#include <memory>


namespace DB
{

/// Bounds waiting for any reply of the broker and every read or write on the socket. Also bounds how long
/// DROP TABLE, the shutdown of an INSERT, or the server shutdown can block when the broker connection is dead.
static constexpr UInt64 RABBITMQ_REQUEST_TIMEOUT_MS = 30000;

/// Bounds the closing handshake of a connection. Short, because nothing is lost if it does not complete.
static constexpr UInt64 RABBITMQ_CLOSE_TIMEOUT_MS = 1000;

struct RabbitMQConfiguration
{
    String host;
    UInt16 port{};
    String username;
    SecretString password;
    String vhost;

    bool secure{};
    /// `rabbitmq_address`: if set, it is used instead of the fields above.
    SecretString connection_string;

    [[nodiscard]] AMQP::ConnectionSettings getConnectionSettings() const;
    [[nodiscard]] String getAddressForLog() const;
};

/// A connection to RabbitMQ with one channel, which `connect` establishes again once it is lost.
/// Not thread-safe: a connection is used by one thread at a time.
class RabbitMQConnection
{
public:
    RabbitMQConnection(const RabbitMQConfiguration & configuration_, LoggerPtr log_);
    ~RabbitMQConnection();

    /// Whether there is a connection, and it has not failed and was not closed by the broker.
    bool isConnected();

    /// Connects, closing the previous connection first. Throws `CANNOT_CONNECT_RABBITMQ`.
    void connect();

    /// Closes the connection gracefully if it is usable, otherwise just closes the socket.
    void disconnect();

    /// The current connection. Requires a successful `connect` before.
    AMQP::Connection & get();

    /// The heartbeat interval in seconds negotiated with the server, 0 if heartbeats are disabled or there is no connection.
    [[nodiscard]] UInt16 getHeartbeatInterval() const { return connection ? connection->getHeartbeatInterval() : 0; }

    [[nodiscard]] String connectionInfoForLog() const { return configuration.getAddressForLog(); }

private:
    const RabbitMQConfiguration configuration;
    LoggerPtr log;
    std::unique_ptr<AMQP::Connection> connection;
};

using RabbitMQConnectionPtr = std::unique_ptr<RabbitMQConnection>;

}
