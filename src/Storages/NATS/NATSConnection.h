#pragma once

#include <Common/Logger.h>

#include <Storages/NATS/NATSHandler.h>

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
public:
    NATSConnection(const NATSConfiguration & configuration_, LoggerPtr log_, NATSOptionsPtr options_);
    ~NATSConnection();

    bool isConnected();

    /// Must bse called from event loop thread
    bool connect();

    /// Must bse called from event loop thread
    bool reconnect();

    void disconnect();

    natsConnection * getConnection() { return connection.get(); }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl() const;

    void connectImpl();

    void disconnectImpl();

    static void disconnectedCallback(natsConnection * nc, void * log);
    static void reconnectedCallback(natsConnection * nc, void * log);

    NATSConfiguration configuration;
    LoggerPtr log;

    NATSOptionsPtr options;
    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)> connection;

    std::mutex mutex;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}
