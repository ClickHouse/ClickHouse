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
    String vhost;

    bool secure;
    String connection_string;
};

class NATSConnectionManager
{

public:
    NATSConnectionManager(const NATSConfiguration & configuration_, Poco::Logger * log_);
    ~NATSConnectionManager() { natsConnection_Destroy(connection); }

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
