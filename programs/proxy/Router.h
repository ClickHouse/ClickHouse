#pragma once

#include <RoutingTable.h>

#include <Common/Logger.h>

#include <condition_variable>
#include <map>
#include <mutex>

namespace DB::Proxy
{

/// Applies the routing table to a connection: resolves the pool, chooses a backend
/// honoring stickiness and load balancing, and runs the configured hooks
/// (unknown route, no backends available, first time a user or a database is seen).
class Router
{
public:
    Router(const ProxyConfiguration & config, bool passive_marking_down_);

    struct Decision
    {
        BackendPoolPtr pool;    /// Null if no rule matched and the listener has no default pool.
        BackendPtr backend;     /// Null if the pool has no alive backends.
    };

    /// May run hooks (shell commands) and wait for backends; call only from a fiber.
    Decision route(const RouteAttributes & attributes, const ListenerConfig & listener);

    /// Route without running any hooks or waiting. Pure lookup, so it is safe to call from a
    /// non-cooperative context (e.g. inside a libssh callback running in thread mode).
    Decision routeStatic(const RouteAttributes & attributes, const ListenerConfig & listener);

    bool needsCredentials(ListenerProtocol protocol) const { return table->needsCredentials(protocol); }
    bool needsQueryType(ListenerProtocol protocol) const { return table->needsQueryType(protocol); }

    /// Whether connect failures may mark a backend as down
    /// (only when active health checks are enabled to bring it back up).
    bool passiveMarkingDown() const { return passive_marking_down; }
    UInt32 failuresToMarkDown() const { return failures_to_mark_down; }

    const std::map<String, BackendPoolPtr> & staticPools() const { return pools; }
    std::vector<BackendPoolPtr> dynamicPoolsSnapshot() const;

private:
    const HooksConfig hooks;
    const StickinessConfig stickiness;
    const bool passive_marking_down;
    const UInt32 failures_to_mark_down;

    std::map<String, BackendPoolPtr> pools;
    std::unique_ptr<IRoutingTable> table;

    /// Pools created from backend templates, keyed by the resolved address.
    mutable std::mutex dynamic_mutex;
    std::map<String, BackendPoolPtr> dynamic_pools;

    /// Coordination of first-seen hooks: the first connection runs the hook,
    /// concurrent connections with the same name wait for its completion.
    std::mutex first_seen_mutex;
    std::condition_variable first_seen_finished;
    std::map<String, bool> seen_users;
    std::map<String, bool> seen_databases;

    LoggerPtr log;

    void runFirstSeenHook(const String & command, const char * kind, const String & value,
        std::map<String, bool> & seen, const RouteAttributes & attributes);
    bool runHook(const String & command, const char * kind, const RouteAttributes & attributes);
    BackendPoolPtr resolvePool(const RouteAttributes & attributes, const ListenerConfig & listener);
    BackendPoolPtr poolForDynamicBackend(const BackendConfig & backend_config, const ListenerConfig & listener);
};

}
