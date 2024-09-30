#include "NATSConnection.h"

#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{

/// disconnectedCallback may be called after connection destroy
LoggerPtr NATSConnection::callback_logger = getLogger("NATSConnection callback");

NATSConnection::NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_, NATSOptionsPtr options_)
    : configuration(configuration_)
    , log(std::move(log_))
    , options(std::move(options_))
    , connection(nullptr, &natsConnection_Destroy)
{
    if (!configuration.username.empty() && !configuration.password.empty())
        natsOptions_SetUserInfo(options.get(), configuration.username.c_str(), configuration.password.c_str());
    if (!configuration.token.empty())
        natsOptions_SetToken(options.get(), configuration.token.c_str());
    if (!configuration.credential_file.empty())
        natsOptions_SetUserCredentialsFromFiles(options.get(), configuration.credential_file.c_str(), nullptr);

    if (configuration.secure)
    {
        natsOptions_SetSecure(options.get(), true);
    }

    // use CLICKHOUSE_NATS_TLS_SECURE=0 env var to skip TLS verification of server cert
    const char * val = std::getenv("CLICKHOUSE_NATS_TLS_SECURE"); // NOLINT(concurrency-mt-unsafe) // this is safe on Linux glibc/Musl, but potentially not safe on other platforms
    std::string tls_secure = val == nullptr ? std::string("1") : std::string(val);
    if (tls_secure == "0")
    {
        natsOptions_SkipServerVerification(options.get(), true);
    }

    if (!configuration.url.empty())
    {
        natsOptions_SetURL(options.get(), configuration.url.c_str());
    }
    else
    {
        const char * servers[configuration.servers.size()];
        for (size_t i = 0; i < configuration.servers.size(); ++i)
        {
            servers[i] = configuration.servers[i].c_str();
        }
        natsOptions_SetServers(options.get(), servers, static_cast<int>(configuration.servers.size()));
    }
    natsOptions_SetMaxReconnect(options.get(), configuration.max_reconnect);
    natsOptions_SetReconnectWait(options.get(), configuration.reconnect_wait);
    natsOptions_SetDisconnectedCB(options.get(), disconnectedCallback, this);
    natsOptions_SetReconnectedCB(options.get(), reconnectedCallback, this);
}
NATSConnection::~NATSConnection()
{
    if (!isDisconnectedImpl())
    {
        natsConnection_Close(connection.get());
    }

    LOG_DEBUG(log, "Destroy connection {} to {}", static_cast<void*>(this), connectionInfoForLog());
}


String NATSConnection::connectionInfoForLog() const
{
    if (!configuration.url.empty())
    {
        return "url : " + configuration.url;
    }
    return "cluster: " + boost::algorithm::join(configuration.servers, ", ");
}

bool NATSConnection::isConnected()
{
    std::lock_guard lock(mutex);
    return isConnectedImpl();
}

bool NATSConnection::isDisconnected()
{
    std::lock_guard lock(mutex);
    return isDisconnectedImpl();
}

bool NATSConnection::isClosed()
{
    std::lock_guard lock(mutex);
    return isClosedImpl();
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
    else if (!isDisconnectedImpl())
        return false;

    disconnectImpl();

    LOG_DEBUG(log, "Trying to restore connection {} to NATS {}", static_cast<void*>(this), connectionInfoForLog());
    connectImpl();

    return isConnectedImpl();
}

void NATSConnection::disconnect()
{
    std::lock_guard lock(mutex);
    disconnectImpl();
}

bool NATSConnection::isConnectedImpl() const
{
    return connection && (natsConnection_Status(connection.get()) == NATS_CONN_STATUS_CONNECTED || natsConnection_IsDraining(connection.get()));
}

bool NATSConnection::isDisconnectedImpl() const
{
    return !connection || (natsConnection_Status(connection.get()) == NATS_CONN_STATUS_DISCONNECTED || natsConnection_IsClosed(connection.get()));
}

bool NATSConnection::isClosedImpl() const
{
    return !connection || natsConnection_IsClosed(connection.get());
}


void NATSConnection::connectImpl()
{
    natsConnection * new_conection = nullptr;
    natsStatus status = natsConnection_Connect(&new_conection, options.get());
    if (status != NATS_OK)
    {
        LOG_DEBUG(log, "New connection {} to {} failed. Nats status text: {}. Last error message: {}",
                  static_cast<void*>(this), connectionInfoForLog(), natsStatus_GetText(status), nats_GetLastError(nullptr));
        return;
    }
    connection.reset(new_conection);

    LOG_DEBUG(log, "New connection {} to {} connected.", static_cast<void*>(this), connectionInfoForLog());
}

void NATSConnection::disconnectImpl()
{
    if (isDisconnectedImpl())
    {
        return;
    }

    natsConnection_Close(connection.get());
}

void NATSConnection::reconnectedCallback(natsConnection *, void * connection)
{
    LOG_DEBUG(callback_logger, "Got reconnected {} to NATS server.", static_cast<void*>(connection));
}

void NATSConnection::disconnectedCallback(natsConnection *, void * connection)
{
    LOG_DEBUG(callback_logger, "Got disconnected {} from NATS server.", connection);
}

}
