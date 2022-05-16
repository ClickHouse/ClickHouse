#pragma once

#include <Storages/NATS/Buffer_fwd.h>
#include <Storages/NATS/NATSHandler.h>
#include <Storages/UVLoop.h>

namespace DB
{

struct NATSConfiguration
{
    String url;
    std::vector<String> servers;

    String username;
    String password;
    String token;

    int max_reconnect;
    int reconnect_wait;

    bool secure;
};

class NATSConnectionManager
{
public:
    NATSConnectionManager(const NATSConfiguration & configuration_, Poco::Logger * log_);
    ~NATSConnectionManager();

    bool isConnected();

    bool connect();

    bool reconnect();

    void disconnect();

    bool closed();

    /// NATSHandler is thread safe. Any public methods can be called concurrently.
    NATSHandler & getHandler() { return event_handler; }
    natsConnection * getConnection() { return connection; }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl() const;

    void connectImpl();

    void disconnectImpl();

    static void disconnectedCallback(natsConnection * nc, void * log);
    static void reconnectedCallback(natsConnection * nc, void * log);

    NATSConfiguration configuration;
    Poco::Logger * log;

    UVLoop loop;
    NATSHandler event_handler;


    natsConnection * connection;
    // true if at any point successfully connected to NATS
    bool has_connection = false;

    std::mutex mutex;
};

using NATSConnectionManagerPtr = std::shared_ptr<NATSConnectionManager>;

}
