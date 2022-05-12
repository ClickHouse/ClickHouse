#include "NATSConnection.h"

#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{

//static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;
static const auto CONNECTED_TO_BUFFER_SIZE = 256;


NATSConnectionManager::NATSConnectionManager(const NATSConfiguration & configuration_, Poco::Logger * log_)
    : configuration(configuration_)
    , log(log_)
    , event_handler(loop.getLoop(), log)
{
}


NATSConnectionManager::~NATSConnectionManager()
{
    if (has_connection)
        natsConnection_Destroy(connection);
}

String NATSConnectionManager::connectionInfoForLog() const
{
    if (!configuration.url.empty()) {
        return "url : " + configuration.url;
    }
    return "cluster: " + boost::algorithm::join(configuration.servers, ", ");
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
    return event_handler.connectionRunning() && connection && !natsConnection_IsClosed(connection);
}

void NATSConnectionManager::connectImpl()
{
    natsOptions * options = event_handler.getOptions();
    natsOptions_SetUserInfo(options, configuration.username.c_str(), configuration.password.c_str());
    if (configuration.secure) {
        natsOptions_SetSecure(options, true);
        natsOptions_SkipServerVerification(options, true);
    }
    if (!configuration.url.empty())
    {
        natsOptions_SetURL(options, configuration.url.c_str());
    } else {
        const char * servers[configuration.servers.size()];
        for (size_t i = 0; i < configuration.servers.size(); ++i) {
            servers[i] = configuration.servers[i].c_str();
        }
        natsOptions_SetServers(options, servers, configuration.servers.size());
    }
    natsOptions_SetMaxReconnect(options, configuration.max_reconnect);
    natsOptions_SetReconnectWait(options, configuration.reconnect_wait);
    natsOptions_SetDisconnectedCB(options, disconnectedCallback, log);
    natsOptions_SetReconnectedCB(options, reconnectedCallback, log);
    auto status = natsConnection_Connect(&connection, options);
    if (status != NATS_OK)
    {
        if (!configuration.url.empty())
            LOG_DEBUG(log, "Failed to connect to NATS on address: {}", configuration.url);
        else
            LOG_DEBUG(log, "Failed to connect to NATS cluster");
        return;
    }

    has_connection = true;
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

void NATSConnectionManager::reconnectedCallback(natsConnection * nc, void * log)
{
    char buffer[CONNECTED_TO_BUFFER_SIZE];
    buffer[0] = '\0';
    natsConnection_GetConnectedUrl(nc, buffer, sizeof(buffer));
    LOG_DEBUG(static_cast<Poco::Logger *>(log), "Got reconnected to NATS server: {}.", buffer);
}

void NATSConnectionManager::disconnectedCallback(natsConnection *, void * log)
{
    LOG_DEBUG(static_cast<Poco::Logger *>(log), "Got disconnected from NATS server.");
}

}
