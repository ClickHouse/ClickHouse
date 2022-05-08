#include "NATSConnection.h"

#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;


NATSConnection::NATSConnection(const NATSConfiguration & configuration_, Poco::Logger * log_)
    : configuration(configuration_)
    , log(log_)
    , event_handler(loop.getLoop(), log)
{
}

String NATSConnection::connectionInfoForLog() const
{
    return configuration.host + ':' + toString(configuration.port);
}

bool NATSConnection::isConnected()
{
    std::lock_guard lock(mutex);
    return isConnectedImpl();
}

bool NATSConnection::connect()
{
    std::lock_guard lock(mutex);
    connectImpl();
    return isConnectedImpl();
}

bool NATSConnection::reconnect()
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

SubscriptionPtr NATSConnection::createSubscription(const std::string& subject)
{
    std::lock_guard lock(mutex);
    natsSubscription * ns;
    natsConnection_SubscribeSync(&ns, connection, subject.c_str());
    return SubscriptionPtr(ns, &natsSubscription_Destroy);
}

void NATSConnection::disconnect()
{
    std::lock_guard lock(mutex);
    disconnectImpl();
}

bool NATSConnection::closed()
{
    std::lock_guard lock(mutex);
    return natsConnection_IsClosed(connection);
}

bool NATSConnection::isConnectedImpl() const
{
    return event_handler.connectionRunning() && !natsConnection_IsClosed(connection);
}

void NATSConnection::connectImpl()
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

    auto cnt_retries = 0;
    while (true)
    {
        event_handler.iterateLoop();

        if (connection->ready() || cnt_retries++ == RETRIES_MAX)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }
}

void NATSConnection::disconnectImpl()
{
    natsConnection_Close(connection);

    /** Connection is not closed immediately (firstly, all pending operations are completed, and then
     *  an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
     */
    size_t cnt_retries = 0;
    while (!closed() && cnt_retries++ != RETRIES_MAX)
        event_handler.iterateLoop();
}

}
