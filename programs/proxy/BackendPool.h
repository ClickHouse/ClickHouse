#pragma once

#include <LoadBalancing.h>

namespace DB::Proxy
{

/// Attributes of an incoming connection extracted before routing.
/// Fields that were not (or could not be) extracted are empty.
struct RouteAttributes
{
    ListenerProtocol protocol = ListenerProtocol::Stream;
    String host;            /// From TLS SNI or the HTTP Host header, without the port.
    String user;
    String database;
    String query_type;      /// select, insert or other.
    String authorized_key;  /// The SSH public key offered by the client, as "<type> <base64>".
    String session_id;      /// From the HTTP URL.
    String peer_address;    /// Client IP address, without the port.
    /// Choose the backend by the peer address even when the pool is not configured to: for a connection
    /// whose later requests (e.g. an encrypted PostgreSQL `CancelRequest`) must reach the same backend.
    bool force_sticky_by_peer_address = false;
};

/// A named group of backends with a load balancing strategy.
class BackendPool
{
public:
    BackendPool(const PoolConfig & config, const StickinessConfig & global_stickiness);

    /// A pool of one ad-hoc backend, created from a routing rule with a backend template.
    BackendPool(String name_, BackendPtr backend, const StickinessConfig & global_stickiness);

    const String & name() const { return pool_name; }
    const std::vector<BackendPtr> & backends() const { return all_backends; }
    std::string_view loadBalancingName() const { return strategy->name(); }

    /// Choose an alive backend honoring stickiness. Returns nullptr if all backends are down.
    BackendPtr choose(const RouteAttributes & attributes) const;

    bool hasAliveBackends() const;

private:
    String pool_name;
    std::vector<BackendPtr> all_backends;
    std::unique_ptr<ILoadBalancingStrategy> strategy;
    StickinessConfig stickiness;
};

using BackendPoolPtr = std::shared_ptr<BackendPool>;

}
