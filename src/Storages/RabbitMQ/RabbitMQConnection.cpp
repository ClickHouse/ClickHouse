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
    return event_handler.connectionRunning() && connection->usable();
}

bool RabbitMQConnection::connect()
{
    std::lock_guard lock(mutex);
    if (configuration.connection_string.empty())
    {
        AMQP::Login login(configuration.username, configuration.password);
        AMQP::Address address(configuration.host, configuration.port, login, configuration.vhost, configuration.secure);
        connection = std::make_unique<AMQP::TcpConnection>(&event_handler, address);
    }
    else
    {
        AMQP::Address address(configuration.connection_string);
        connection = std::make_unique<AMQP::TcpConnection>(&event_handler, address);
    }

    auto cnt_retries = 0;
    while (!connection->ready() && cnt_retries++ != RETRIES_MAX)
    {
        event_handler.iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }
    return event_handler.connectionRunning();
}

bool RabbitMQConnection::reconnect()
{
    disconnect();
    {
        /// This will force immediate closure if not yet closed
        std::lock_guard lock(mutex);
        if (!connection->closed())
            connection->close(true);
    }
    LOG_TRACE(log, "Trying to restore connection to {}", connectionInfoForLog());
    return connect();
}

ChannelPtr RabbitMQConnection::createChannel()
{
    std::lock_guard lock(mutex);
    return std::make_unique<AMQP::TcpChannel>(connection.get());
}

void RabbitMQConnection::disconnect(bool immediately)
{
    std::lock_guard lock(mutex);
    connection->close(immediately);

    /** Connection is not closed immediately (firstly, all pending operations are completed, and then
     *  an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
     */
    size_t cnt_retries = 0;
    while (!connection->closed() && cnt_retries++ != RETRIES_MAX)
        event_handler.iterateLoop();
}

}
