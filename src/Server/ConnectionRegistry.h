#pragma once

#include <atomic>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <base/types.h>
#include <Common/IPv6ToBinary.h>
#include <Common/SharedMutex.h>
#include <Poco/Net/IPAddress.h>


namespace DB
{

struct ConnectionInfo
{
    UInt64 connection_id = 0;
    String protocol;            /// "TCP" or "HTTP"
    Poco::Net::IPAddress client_address;
    UInt16 client_port = 0;
    UInt16 server_port = 0;
    String user;
    String status;              /// "active" or "idle"
    String query_id;
    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    time_t connected_time = 0;
    time_t last_query_time = 0; /// 0 if no query has been executed yet
};


/// Global registry of active client connections.
/// Each connection handler registers itself on construction via add() and holds
/// the returned Handle.  The Handle deregisters the connection on destruction.
class ConnectionRegistry
{
public:
    static ConnectionRegistry & instance();

    class Handle
    {
    public:
        Handle() = default;
        Handle(ConnectionRegistry & registry_, UInt64 id_) : registry(&registry_), id(id_) {}

        ~Handle()
        {
            if (registry)
                registry->remove(id);
        }

        Handle(const Handle &) = delete;
        Handle & operator=(const Handle &) = delete;

        Handle(Handle && other) noexcept : registry(other.registry), id(other.id)
        {
            other.registry = nullptr;
        }
        Handle & operator=(Handle && other) noexcept
        {
            if (this != &other)
            {
                if (registry)
                    registry->remove(id);
                registry = other.registry;
                id = other.id;
                other.registry = nullptr;
            }
            return *this;
        }

        void setActive(const String & query_id_);
        void setIdle();

    private:
        ConnectionRegistry * registry = nullptr;
        UInt64 id = 0;
    };

    /// Must be called once at server startup (from attachSystemTablesServer) when
    /// collect_connection_metrics is enabled.  Until this is called, add() is a no-op.
    void enable();

    Handle add(ConnectionInfo info);

    /// Returns a snapshot of all currently registered connections.
    std::vector<ConnectionInfo> list() const;

private:
    void remove(UInt64 id);
    void update(UInt64 id, const String & status, const String & query_id, time_t last_query_time);

    std::atomic<bool> enabled{false};
    mutable DB::SharedMutex mutex;
    std::unordered_map<UInt64, ConnectionInfo> connections;
    std::atomic<UInt64> next_id{1};
};

}
