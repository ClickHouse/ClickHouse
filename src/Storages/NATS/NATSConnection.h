#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <nats.h>

#include <mutex>

namespace DB
{

struct NATSConfiguration
{
    String url;
    std::vector<String> servers;

    String username;
    String password;
    String token;
    String credential_file;

    UInt64 max_connect_tries;
    int reconnect_wait;

    bool secure;
};

using NATSOptionsPtr = std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)>;

class NATSConnection
{
    using Lock = std::lock_guard<std::mutex>;

public:
    NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_, NATSOptionsPtr options_);
    ~NATSConnection();

    bool isConnected();
    bool isDisconnected();
    bool isClosed();

    /// Must be called from event loop thread
    bool connect();

    void disconnect();

    natsConnection * getConnection() { return connection.get(); }
    int getReconnectWait() const { return configuration.reconnect_wait; }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl(const Lock & connection_lock) const;
    bool isDisconnectedImpl(const Lock & connection_lock) const;
    bool isClosedImpl(const Lock & connection_lock) const;

    void connectImpl(const Lock & connection_lock);

    void disconnectImpl(const Lock & connection_lock);

    static void disconnectedCallback(natsConnection * nc, void * connection);
    static void reconnectedCallback(natsConnection * nc, void * connection);

    NATSConfiguration configuration;
    LoggerPtr log;

    NATSOptionsPtr options;
    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)> connection;

    std::mutex mutex;

    /// disconnectedCallback may be called after connection destroy
    static LoggerPtr callback_logger;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}
