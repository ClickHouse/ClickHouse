#pragma once

#include <atomic>
#include <ctime>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Poco/Net/IPAddress.h>


namespace DB
{

/// The properties of a client connection that do not change after registration.
struct ConnectionInfo
{
    String protocol;            /// "TCP" or "HTTP"
    UInt16 server_port = 0;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    time_t connected_time = 0;
};


/// The properties of the client of a connection.
/// For TCP, these properties do not change. For HTTP, each request sets these properties again.
/// A reverse proxy can send the requests of different clients and users on one keep-alive connection.
struct ConnectionPeer
{
    String user;
    Poco::Net::IPAddress client_address;
    UInt16 client_port = 0;
    String client_name;
};


/// A copy of a registered connection at one point in time. `ConnectionRegistry::list` returns these copies.
struct ConnectionSnapshot
{
    UInt64 connection_id = 0;
    ConnectionInfo info;
    ConnectionPeer peer;
    bool is_active = false;     /// true when a query runs, false when the connection waits for the next query
    String query_id;            /// empty when the connection is idle
    time_t last_query_time = 0; /// 0 if the connection did not run a query
};


/// A registered connection. It has two parts:
/// - A constant part.
/// - A part that changes on each query. A mutex of this connection protects this part.
/// Thus, updates on different connections do not wait for one registry-wide lock.
class ConnectionState
{
public:
    ConnectionState(UInt64 connection_id_, ConnectionInfo info_, ConnectionPeer peer_);

    /// Sets the connection to active. The client properties do not change.
    void setActive(const String & query_id_);
    /// Sets the connection to active and replaces the client properties.
    void setActive(const String & query_id_, ConnectionPeer peer_);
    void setIdle();

    ConnectionSnapshot getSnapshot() const;

    const UInt64 connection_id;
    const ConnectionInfo info;

private:
    mutable std::mutex mutex;
    ConnectionPeer peer TSA_GUARDED_BY(mutex);
    bool is_active TSA_GUARDED_BY(mutex) = false;
    String query_id TSA_GUARDED_BY(mutex);
    time_t last_query_time TSA_GUARDED_BY(mutex) = 0;
};

using ConnectionStatePtr = std::shared_ptr<ConnectionState>;


class ConnectionRegistry;

/// Owns the registration of a connection in `ConnectionRegistry`. The destructor removes the registration.
/// A default-constructed or moved-from handle is empty. The methods of an empty handle do nothing.
class ConnectionHandle
{
public:
    ConnectionHandle() = default;
    ConnectionHandle(ConnectionRegistry & registry_, ConnectionStatePtr state_);
    ~ConnectionHandle();

    ConnectionHandle(const ConnectionHandle &) = delete;
    ConnectionHandle & operator=(const ConnectionHandle &) = delete;
    ConnectionHandle(ConnectionHandle && other) noexcept;
    ConnectionHandle & operator=(ConnectionHandle && other) noexcept;

    explicit operator bool() const { return state != nullptr; }

    void setActive(const String & query_id);
    void setActive(const String & query_id, ConnectionPeer peer);
    void setIdle();

private:
    void reset();

    ConnectionRegistry * registry = nullptr;
    ConnectionStatePtr state;
};


/// The registration of one HTTP connection. All requests of a keep-alive connection use the same registration.
/// `HTTPServerConnection` serves all HTTP interfaces: queries, interserver replication, Prometheus, Keeper, and others.
/// Thus, `HTTPServerConnection` does not register the connection.
/// `HTTPServerConnection` keeps this object until the socket closes.
/// `HTTPHandler` registers the connection in this object when the connection gets its first query.
/// The registration stays after each request and goes away when the socket closes.
/// Thus, `system.connections` shows a keep-alive connection as idle between the requests.
struct HTTPConnectionRegistration
{
    time_t connected_time = 0;
    ConnectionHandle handle;
};


/// The global registry of client connections of the native TCP and HTTP query interfaces.
/// `system.connections` shows the contents of this registry.
/// The registry-wide lock protects only these operations: add a connection, remove a connection, and list the connections.
/// A query does not use this lock.
class ConnectionRegistry
{
public:
    static ConnectionRegistry & instance();

    /// Call this function one time at server start (from `attachSystemTablesServer`) if `collect_connection_metrics` is `true`.
    /// Before this call, `add` returns an empty handle.
    void enable();
    bool isEnabled() const { return enabled.load(std::memory_order_relaxed); }

    /// Registers a connection. The connection is idle until the first call to `setActive`.
    ConnectionHandle add(ConnectionInfo info, ConnectionPeer peer);

    /// Returns a copy of all registered connections.
    std::vector<ConnectionSnapshot> list() const;

private:
    friend class ConnectionHandle;

    void remove(UInt64 id);

    std::atomic<bool> enabled{false};
    std::atomic<UInt64> next_id{1};

    mutable std::mutex mutex;
    std::unordered_map<UInt64, ConnectionStatePtr> connections TSA_GUARDED_BY(mutex);
};

}
