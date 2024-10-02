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

    int max_reconnect;
    int reconnect_wait;

    bool secure;
};

class NATSConnection
{
    using NATSOptionsPtr = std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)>;

public:
    NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_);

    bool isConnected();
    bool isDisconnected();
    bool isClosed();

    /// Must bse called from event loop thread
    bool connect();

    /// Must bse called from event loop thread
    bool reconnect();

    void disconnect();

    natsConnection * getConnection() { return connection.get(); }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl() const;
    bool isDisconnectedImpl() const;
    bool isClosedImpl() const;

    void connectImpl();

    void disconnectImpl();

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
