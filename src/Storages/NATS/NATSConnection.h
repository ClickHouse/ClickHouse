#pragma once

#include <Storages/NATS/UVLoop.h>
#include <Storages/NATS/NATSHandler.h>


namespace DB
{

struct NATSConfiguration
{
    String url;
    String host;
    UInt16 port;
    String username;
    String password;
    String vhost;

    bool secure;
    String connection_string;
};

using SubscriptionPtr = std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;

class NATSConnection
{

public:
    NATSConnection(const NATSConfiguration & configuration_, Poco::Logger * log_);

    bool isConnected();

    bool connect();

    bool reconnect();

    void disconnect();

    bool closed();

    SubscriptionPtr createSubscription(const std::string& subject);

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

using NATSConnectionPtr = std::unique_ptr<NATSConnection>;

}
