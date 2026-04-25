#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <nats.h>

#include <atomic>
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

    /// Monotonically increasing counter incremented by the libnats reconnect
    /// callback every time the underlying TCP connection is re-established to
    /// a NATS server. Used by `StorageNATS` to detect reconnects and re-issue
    /// JetStream pull subscriptions, which are NOT auto-restored by libnats
    /// (plain `Subscribe` / `QueueSubscribe` are auto-restored by libnats and
    /// do not need re-subscription).
    uint64_t getReconnectCount() const noexcept { return reconnect_count.load(std::memory_order_acquire); }

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

    /// Reconnect counter, incremented from `reconnectedCallback` on the
    /// libnats reconnect thread and read from the streaming task thread.
    /// Safe because `natsConnection_Destroy` (called from the connection
    /// unique_ptr's deleter in `~NATSConnection`) synchronously joins the
    /// libnats threads before returning, so the callback cannot fire after
    /// the `NATSConnection` is destroyed.
    std::atomic<uint64_t> reconnect_count{0};

    /// disconnectedCallback may be called after connection destroy
    static LoggerPtr callback_logger;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}

