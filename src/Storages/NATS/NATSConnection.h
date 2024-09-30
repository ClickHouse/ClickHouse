#pragma once

#include <Common/Logger.h>

#include <Storages/NATS/NATSHandler.h>

#include <future>
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

    std::optional<std::shared_future<void>> disconnectImpl();

    static void disconnectedCallback(natsConnection * nc, void * this_);
    static void reconnectedCallback(natsConnection * nc, void * this_);

    NATSConfiguration configuration;
    LoggerPtr log;

    NATSOptionsPtr options;
    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)> connection;

    std::mutex mutex;

    std::optional<std::promise<void>> connection_closed_promise;
    std::optional<std::shared_future<void>> connection_closed_future;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}
