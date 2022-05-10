#pragma once

#include <Storages/NATS/UVLoop.h>
#include <Storages/NATS/NATSHandler.h>
#include <Storages/NATS/Buffer_fwd.h>

namespace DB
{

struct NATSConfiguration
{
    String host;
    UInt16 port;
    String username;
    String password;

    int max_reconnect;
    int reconnect_wait;

    bool secure;
    String connection_string;
};

class NATSConnectionManager
{

public:
    NATSConnectionManager(const NATSConfiguration & configuration_, Poco::Logger * log_);
    ~NATSConnectionManager() { natsConnection_Destroy(connection); }

    natsConnection * getConnection() { return connection; }

    bool isConnected();

    bool connect();

    bool reconnect();

    void disconnect();

    bool closed();

    SubscriptionPtr createSubscription(const std::string& subject, natsMsgHandler handler, ReadBufferFromNATSConsumer * consumer);

    /// NATSHandler is thread safe. Any public methods can be called concurrently.
    NATSHandler & getHandler() { return event_handler; }

    String connectionInfoForLog() const;

private:
    bool isConnectedImpl() const;

    void connectImpl();

    void disconnectImpl();

    static void disconnectedCallback(natsConnection * nc, void * storage);
    static void reconnectedCallback(natsConnection * nc, void * storage);

    NATSConfiguration configuration;
    Poco::Logger * log;

    UVLoop loop;
    NATSHandler event_handler;

    natsConnection * connection;
    natsStatus status;
    std::mutex mutex;
};

using NATSConnectionManagerPtr = std::shared_ptr<NATSConnectionManager>;

}
