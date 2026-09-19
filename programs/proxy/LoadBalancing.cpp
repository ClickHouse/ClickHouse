#include <LoadBalancing.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

#include <atomic>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}
}

namespace DB::Proxy
{

namespace
{

class RandomStrategy : public ILoadBalancingStrategy
{
public:
    std::string_view name() const override { return "random"; }

    BackendPtr choose(const std::vector<BackendPtr> & candidates) override
    {
        UInt64 total_weight = 0;
        for (const auto & backend : candidates)
            total_weight += backend->config().weight;

        UInt64 point = std::uniform_int_distribution<UInt64>(0, total_weight - 1)(thread_local_rng);
        for (const auto & backend : candidates)
        {
            if (point < backend->config().weight)
                return backend;
            point -= backend->config().weight;
        }
        return candidates.back();
    }
};

class RoundRobinStrategy : public ILoadBalancingStrategy
{
public:
    std::string_view name() const override { return "round_robin"; }

    BackendPtr choose(const std::vector<BackendPtr> & candidates) override
    {
        return candidates[counter.fetch_add(1, std::memory_order_relaxed) % candidates.size()];
    }

private:
    std::atomic<UInt64> counter {0};
};

class LeastConnectionsStrategy : public ILoadBalancingStrategy
{
public:
    std::string_view name() const override { return "least_connections"; }

    BackendPtr choose(const std::vector<BackendPtr> & candidates) override
    {
        BackendPtr best;
        double best_load = 0;
        for (const auto & backend : candidates)
        {
            double load = static_cast<double>(backend->activeConnections()) / backend->config().weight;
            if (!best || load < best_load)
            {
                best = backend;
                best_load = load;
            }
        }
        return best;
    }
};

class LowestLatencyStrategy : public ILoadBalancingStrategy
{
public:
    std::string_view name() const override { return "lowest_latency"; }

    BackendPtr choose(const std::vector<BackendPtr> & candidates) override
    {
        BackendPtr best;
        double best_latency = 0;
        for (const auto & backend : candidates)
        {
            double raw_latency = backend->checkLatencyMs() > 0 ? backend->checkLatencyMs() : backend->connectLatencyMs();
            double latency = raw_latency / backend->config().weight;
            if (!best || latency < best_latency)
            {
                best = backend;
                best_latency = latency;
            }
        }
        return best;
    }
};

class LeastResourcesStrategy : public ILoadBalancingStrategy
{
public:
    std::string_view name() const override { return "least_resources"; }

    BackendPtr choose(const std::vector<BackendPtr> & candidates) override
    {
        /// Backends with unknown resource usage (not polled yet) are preferred to loaded ones:
        /// treat unknown as zero. Ties are broken by the number of active connections.
        BackendPtr best;
        double best_cpu = 0;
        for (const auto & backend : candidates)
        {
            double cpu = std::max(backend->cpuUsage(), 0.0) / backend->config().weight;
            if (!best || cpu < best_cpu
                || (cpu == best_cpu && backend->activeConnections() < best->activeConnections()))
            {
                best = backend;
                best_cpu = cpu;
            }
        }
        return best;
    }
};

}

std::unique_ptr<ILoadBalancingStrategy> ILoadBalancingStrategy::create(const String & strategy_name)
{
    if (strategy_name == "random")
        return std::make_unique<RandomStrategy>();
    if (strategy_name == "round_robin")
        return std::make_unique<RoundRobinStrategy>();
    if (strategy_name == "least_connections")
        return std::make_unique<LeastConnectionsStrategy>();
    if (strategy_name == "lowest_latency")
        return std::make_unique<LowestLatencyStrategy>();
    if (strategy_name == "least_resources")
        return std::make_unique<LeastResourcesStrategy>();
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown load balancing strategy '{}'. Supported strategies: random, round_robin, least_connections, lowest_latency, least_resources",
        strategy_name);
}

BackendPtr chooseByConsistentHash(const std::vector<BackendPtr> & candidates, const String & key)
{
    /// Weighted rendezvous (highest random weight) hashing: a backend receives keys in proportion
    /// to its weight, and each key moves only when its chosen backend is removed. The score is
    /// monotonically increasing in the hash, so with equal weights the winner is the same backend
    /// that plain rendezvous hashing (argmax of the hash) would pick.
    BackendPtr best;
    double best_score = 0;
    for (const auto & backend : candidates)
    {
        SipHash hash;
        hash.update(key);
        hash.update(backend->name());
        /// The top 53 bits of the hash, mapped to a double strictly inside (0, 1),
        /// so the logarithm below is finite and negative.
        const double uniform = (static_cast<double>(hash.get64() >> 11) + 0.5) * 0x1.0p-53;
        const double score = -static_cast<double>(backend->config().weight) / std::log(uniform);
        if (!best || score > best_score)
        {
            best = backend;
            best_score = score;
        }
    }
    return best;
}

}
