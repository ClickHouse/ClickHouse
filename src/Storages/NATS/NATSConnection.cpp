#include "NATSConnection.h"

#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

//static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;


NATSConnectionManager::NATSConnectionManager(const NATSConfiguration & configuration_, Poco::Logger * log_)
    : configuration(configuration_)
    , log(log_)
    , event_handler(loop.getLoop(), log)
{
}

String NATSConnectionManager::connectionInfoForLog() const
{
    return configuration.host + ':' + toString(configuration.port);
}

bool NATSConnectionManager::isConnected()
{
    std::lock_guard lock(mutex);
    return isConnectedImpl();
}

bool NATSConnectionManager::connect()
{
    std::lock_guard lock(mutex);
    connectImpl();
    return isConnectedImpl();
}

bool NATSConnectionManager::reconnect()
{
    std::lock_guard lock(mutex);
    if (isConnectedImpl())
        return true;

    disconnectImpl();

    LOG_DEBUG(log, "Trying to restore connection to {}", connectionInfoForLog());
    connectImpl();

    return isConnectedImpl();
}

SubscriptionPtr NATSConnectionManager::createSubscription(const std::string& subject, natsMsgHandler handler, ReadBufferFromNATSConsumer * consumer)
{
    std::lock_guard lock(mutex);
    natsSubscription * ns;
    status = natsConnection_Subscribe(&ns, connection, subject.c_str(), handler, static_cast<void *>(consumer));
    if (status == NATS_OK)
        status = natsSubscription_SetPendingLimits(ns, -1, -1);
    if (status == NATS_OK)
        LOG_DEBUG(log, "Subscribed to subject {}", subject);
    return SubscriptionPtr(ns, &natsSubscription_Destroy);
}

void NATSConnectionManager::disconnect()
{
    std::lock_guard lock(mutex);
    disconnectImpl();
}

bool NATSConnectionManager::closed()
{
    std::lock_guard lock(mutex);
    return natsConnection_IsClosed(connection);
}

bool NATSConnectionManager::isConnectedImpl() const
{
    return event_handler.connectionRunning() && !natsConnection_IsClosed(connection) && status == natsStatus::NATS_OK;
}

void NATSConnectionManager::connectImpl()
{
    natsOptions * options = event_handler.getOptions();
    natsOptions_SetUserInfo(options, configuration.username.c_str(), configuration.password.c_str());
    if (configuration.secure) {
        natsOptions_SetSecure(options, true);
        natsOptions_SkipServerVerification(options, true);
    }
    std::string address;
    if (configuration.connection_string.empty())
    {
        address = configuration.host + ":" + std::to_string(configuration.port);
    }
    else
    {
        address = configuration.connection_string;
    }
    natsOptions_SetURL(options, address.c_str());
    status = natsConnection_Connect(&connection, options);
    if (status != NATS_OK)
    {
        LOG_DEBUG(log, "Failed to connect to NATS on address: {}", address);
        return;
    }

    event_handler.changeConnectionStatus(true);
}

void NATSConnectionManager::disconnectImpl()
{
    natsConnection_Close(connection);

    /** Connection is not closed immediately (firstly, all pending operations are completed, and then
     *  an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
     */
    size_t cnt_retries = 0;
    while (!natsConnection_IsClosed(connection) && cnt_retries++ != RETRIES_MAX)
        event_handler.iterateLoop();

    event_handler.changeConnectionStatus(false);
}

}
