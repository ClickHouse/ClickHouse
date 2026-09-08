#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <nats.h>

#include <mutex>

namespace DB
{

/// `nats_GetLastError` returns a null pointer when the thread has not recorded an error yet, and
/// formatting a null `const char *` throws `fmt::format_error` instead of reporting the failure
/// that is being logged. Never hand its result to a format string directly.
inline const char * getNATSLastError()
{
    const char * last_error = nats_GetLastError(nullptr);
    return last_error ? last_error : "none";
}

struct NATSConfiguration
{
    String url;
    std::vector<String> servers;

    String username;
    String password;
    String token;
    String credential_file;
    String credentials;

    UInt64 max_connect_tries{};
    int reconnect_wait{};

    bool secure{};
};

using NATSOptionsPtr = std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)>;
using NATSStatisticsPtr = std::unique_ptr<natsStatistics, decltype(&natsStatistics_Destroy)>;

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

    /// How many times the client has re-established this connection. The client restores a
    /// subscription itself, but only its `SUB` line, so anything a subscription was waiting for on
    /// the broker is gone: whoever needs it back has to notice this count changing.
    UInt64 getReconnectCount();

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
    NATSStatisticsPtr statistics;
    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)> connection;

    std::mutex mutex;

    /// disconnectedCallback may be called after connection destroy
    static LoggerPtr callback_logger;
};

using NATSConnectionPtr = std::shared_ptr<NATSConnection>;

}
