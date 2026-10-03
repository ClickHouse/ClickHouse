#pragma once

#include <Backend.h>

#include <memory>
#include <vector>

namespace DB::Proxy
{

/// A strategy choosing a backend among the currently alive ones.
/// Strategies are shared between connections and must be thread-safe.
class ILoadBalancingStrategy
{
public:
    virtual ~ILoadBalancingStrategy() = default;

    virtual std::string_view name() const = 0;

    /// Choose one of the candidates. The candidates list is never empty.
    virtual BackendPtr choose(const std::vector<BackendPtr> & candidates) = 0;

    /// Create a strategy by name: random, round_robin, least_connections, lowest_latency, least_resources.
    static std::unique_ptr<ILoadBalancingStrategy> create(const String & strategy_name);
};

/// Consistent choice by a key: the same key maps to the same backend as long as it stays alive
/// (rendezvous hashing, stable under adding and removing backends).
BackendPtr chooseByConsistentHash(const std::vector<BackendPtr> & candidates, const String & key);

}
