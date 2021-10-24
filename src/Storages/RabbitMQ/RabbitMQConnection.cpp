#include "RabbitMQConnection.h"

#include <common/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;


RabbitMQConnection::RabbitMQConnection(const RabbitMQConfiguration & configuration_, Poco::Logger * log_)
    : configuration(configuration_)
    , log(log_)
    , event_handler(loop.getLoop(), log)
{
}

String RabbitMQConnection::connectionInfoForLog() const
{
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

    disconnectImpl();

    /// This will force immediate closure if not yet closed
    if (!connection->closed())
        connection->close(true);

    LOG_DEBUG(log, "Trying to restore connection to {}", connectionInfoForLog());
    connectImpl();

    return isConnectedImpl();
}

ChannelPtr RabbitMQConnection::createChannel()
{
    std::lock_guard lock(mutex);
    return std::make_unique<AMQP::TcpChannel>(connection.get());
}

void RabbitMQConnection::disconnect(bool immediately)
{
    std::lock_guard lock(mutex);
    disconnectImpl(immediately);
}

void RabbitMQConnection::heartbeat()
{
    std::lock_guard lock(mutex);
    connection->heartbeat();
}

bool RabbitMQConnection::closed()
{
    std::lock_guard lock(mutex);
    return connection->closed();
}

bool RabbitMQConnection::isConnectedImpl() const
{
    return event_handler.connectionRunning() && connection->usable();
}

void RabbitMQConnection::connectImpl()
{
    LOG_DEBUG(log, "Connecting to: {}:{} (user: {})", configuration.host, configuration.port, configuration.username);
    AMQP::Login login(configuration.username, configuration.password);
    AMQP::Address address(configuration.host, configuration.port, login, configuration.vhost);
    connection = std::make_unique<AMQP::TcpConnection>(&event_handler, address);

    auto cnt_retries = 0;
    while (true)
    {
        event_handler.iterateLoop();

        if (connection->ready() || cnt_retries++ == RETRIES_MAX)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }
}

void RabbitMQConnection::disconnectImpl(bool immediately)
{
    connection->close(immediately);

    /** Connection is not closed immediately (firstly, all pending operations are completed, and then
     *  an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
     */
    size_t cnt_retries = 0;
    while (!connection->closed() && cnt_retries++ != RETRIES_MAX)
        event_handler.iterateLoop();
}

}
