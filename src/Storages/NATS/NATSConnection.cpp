#include "NATSConnection.h"

#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

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
    if (!configuration.url.empty())
    {
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

    LOG_DEBUG(log, "Trying to restore connection to NATS {}", connectionInfoForLog());
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
    return connection && has_connection && !natsConnection_IsClosed(connection);
}

void NATSConnectionManager::connectImpl()
{
    natsOptions * options = event_handler.getOptions();
    if (!configuration.username.empty() && !configuration.password.empty())
        natsOptions_SetUserInfo(options, configuration.username.c_str(), configuration.password.c_str());
    if (!configuration.token.empty())
        natsOptions_SetToken(options, configuration.token.c_str());

    if (configuration.secure)
    {
        natsOptions_SetSecure(options, true);
        natsOptions_SkipServerVerification(options, true);
    }
    if (!configuration.url.empty())
    {
        natsOptions_SetURL(options, configuration.url.c_str());
    }
    else
    {
        const char * servers[configuration.servers.size()];
        for (size_t i = 0; i < configuration.servers.size(); ++i)
        {
            servers[i] = configuration.servers[i].c_str();
        }
        natsOptions_SetServers(options, servers, configuration.servers.size());
    }
    natsOptions_SetMaxReconnect(options, configuration.max_reconnect);
    natsOptions_SetReconnectWait(options, configuration.reconnect_wait);
    natsOptions_SetDisconnectedCB(options, disconnectedCallback, log);
    natsOptions_SetReconnectedCB(options, reconnectedCallback, log);
    natsStatus status;
    {
        auto lock = event_handler.setThreadLocalLoop();
        status = natsConnection_Connect(&connection, options);
    }
    if (status == NATS_OK)
        has_connection = true;
    else
        LOG_DEBUG(log, "New connection to {} failed. Nats status text: {}. Last error message: {}",
                  connectionInfoForLog(), natsStatus_GetText(status), nats_GetLastError(nullptr));
}

void NATSConnectionManager::disconnectImpl()
{
    if (!has_connection)
        return;

    natsConnection_Close(connection);

    size_t cnt_retries = 0;
    while (!natsConnection_IsClosed(connection) && cnt_retries++ != RETRIES_MAX)
        event_handler.iterateLoop();
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
