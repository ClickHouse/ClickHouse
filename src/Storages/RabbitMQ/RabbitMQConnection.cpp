#include <Storages/RabbitMQ/RabbitMQConnection.h>

#include <amqpcpp.h>
#include <thread>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

static const auto CONNECT_SLEEP = 200;
static const auto DISCONNECT_SLEEP = 10;
static const auto RETRIES_MAX = 20;


RabbitMQConnection::RabbitMQConnection(const RabbitMQConfiguration & configuration_, LoggerPtr log_)
    : configuration(configuration_)
    , log(log_)
    , event_handler(loop.getLoop(), log)
{
}

RabbitMQConnection::~RabbitMQConnection()
{
    /// `connection` is destroyed before `event_handler`, so the loop thread must be gone by then.
    event_handler.stopLoop();
}

String RabbitMQConnection::connectionInfoForLog() const
{
    if (!configuration.connection_string.empty())
    {
        AMQP::Address address(configuration.connection_string);
        return address.hostname() + ":" + toString(address.port());
    }
    return configuration.host + ':' + toString(configuration.port);
}

bool RabbitMQConnection::isConnected()
{
    std::lock_guard lock(mutex);
    return isConnectedImpl();
}

bool RabbitMQConnection::connect()
{
    std::lock_guard lock(mutex);
    connectImpl();
    return isConnectedImpl();
}

bool RabbitMQConnection::reconnect()
{
    std::lock_guard lock(mutex);
    if (isConnectedImpl())
        return true;

    /// Close at once, so each channel reports an error and its consumer replaces it. After a graceful close
    /// (possible after the loop thread died) the consumers would keep their channels and receive nothing.
    event_handler.run([this]
    {
        if (!connection->closed())
            connection->close(true);
    });

    LOG_DEBUG(log, "Trying to restore connection to {}", connectionInfoForLog());
    connectImpl();

    return isConnectedImpl();
}

ChannelPtr RabbitMQConnection::createChannel()
{
    return event_handler.run([this] { return std::make_unique<AMQP::TcpChannel>(connection.get()); });
}

void RabbitMQConnection::disconnect(bool immediately)
{
    std::lock_guard lock(mutex);
    disconnectImpl(immediately);
}

void RabbitMQConnection::heartbeat()
{
    std::lock_guard lock(mutex);
    event_handler.run([this] { connection->heartbeat(); });
}

bool RabbitMQConnection::isConnectedImpl()
{
    return event_handler.run([this] { return event_handler.connectionRunning() && connection->usable(); });
}

void RabbitMQConnection::connectImpl()
{
    event_handler.run([this]
    {
        if (configuration.connection_string.empty())
        {
            LOG_DEBUG(log, "Connecting to: {}:{} (user: {})", configuration.host, configuration.port, configuration.username);
            AMQP::Login login(configuration.username, configuration.password);
            AMQP::Address address(configuration.host, configuration.port, login, configuration.vhost, configuration.secure);
            connection = std::make_unique<AMQP::TcpConnection>(&event_handler, address);
        }
        else
        {
            AMQP::Address address(configuration.connection_string);
            connection = std::make_unique<AMQP::TcpConnection>(&event_handler, address);
        }
    });

    waitFor([this] { return connection->ready(); }, CONNECT_SLEEP);
}

void RabbitMQConnection::disconnectImpl(bool immediately)
{
    event_handler.run([&] { connection->close(immediately); });

    /** Connection is not closed immediately (firstly, all pending operations are completed, and then
     *  an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
     */
    /// Driving the loop inline needs no pause between turns; a loop thread needs time to process the close.
    waitFor([this] { return connection->closed(); }, event_handler.loopRunning() ? DISCONNECT_SLEEP : 0);
}

void RabbitMQConnection::waitFor(std::function<bool()> done, UInt64 sleep_ms)
{
    for (size_t attempt = 0;; ++attempt)
    {
        event_handler.iterateLoop();

        if (event_handler.run(done) || attempt == RETRIES_MAX)
            return;

        if (sleep_ms)
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
}

}
