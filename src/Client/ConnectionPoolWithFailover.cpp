#include <Client/ConnectionPoolWithFailover.h>
#include <Client/ConnectionEstablisher.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Common/BitHelpers.h>
#include <Common/quoteString.h>
#include <base/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>

#include <IO/ConnectionTimeouts.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}


ConnectionPoolWithFailover::ConnectionPoolWithFailover(
        ConnectionPoolPtrs nested_pools_,
        LoadBalancing load_balancing,
        time_t decrease_error_period_,
        size_t max_error_cap_)
    : Base(std::move(nested_pools_), decrease_error_period_, max_error_cap_, &Poco::Logger::get("ConnectionPoolWithFailover"))
    , get_priority_load_balancing(load_balancing)
{
    const std::string & local_hostname = getFQDNOrHostName();

    get_priority_load_balancing.hostname_differences.resize(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i]);
        get_priority_load_balancing.hostname_differences[i] = getHostNameDifference(local_hostname, connection_pool.getHost());
    }
}

IConnectionPool::Entry ConnectionPoolWithFailover::get(const ConnectionTimeouts & timeouts,
                                                       const Settings * settings,
                                                       bool /*force_connected*/)
{
    if (nested_pools.empty())
        throw DB::Exception(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot get connection from ConnectionPoolWithFailover cause nested pools are empty");

    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings);
    };

    size_t offset = 0;
    LoadBalancing load_balancing = get_priority_load_balancing.load_balancing;
    if (settings)
    {
        offset = settings->load_balancing_first_offset % nested_pools.size();
        load_balancing = LoadBalancing(settings->load_balancing);
    }

    GetPriorityFunc get_priority = get_priority_load_balancing.getPriorityFunc(load_balancing, offset, nested_pools.size());

    UInt64 max_ignored_errors = settings ? settings->distributed_replica_max_ignored_errors.value : 0;
    bool fallback_to_stale_replicas = settings ? settings->fallback_to_stale_replicas_for_distributed_queries.value : true;

    return Base::get(max_ignored_errors, fallback_to_stale_replicas, try_get_entry, get_priority);
}

Int64 ConnectionPoolWithFailover::getPriority() const
{
    return (*std::max_element(nested_pools.begin(), nested_pools.end(), [](const auto &a, const auto &b)
    {
        return a->getPriority() - b->getPriority();
    }))->getPriority();
}

ConnectionPoolWithFailover::Status ConnectionPoolWithFailover::getStatus() const
{
    const auto [states, pools, error_decrease_time] = getPoolExtendedStates();
    // NOTE: to avoid data races do not touch any data of ConnectionPoolWithFailover or PoolWithFailoverBase in the code below.

    assert(states.size() == pools.size());

    ConnectionPoolWithFailover::Status result;
    result.reserve(states.size());
    const time_t since_last_error_decrease = time(nullptr) - error_decrease_time;
    /// Update error_count and slowdown_count in states to return actual information.
    auto updated_states = states;
    auto updated_error_decrease_time = error_decrease_time;
    Base::updateErrorCounts(updated_states, updated_error_decrease_time);
    for (size_t i = 0; i < states.size(); ++i)
    {
        const auto rounds_to_zero_errors = states[i].error_count ? bitScanReverse(states[i].error_count) + 1 : 0;
        const auto rounds_to_zero_slowdowns = states[i].slowdown_count ? bitScanReverse(states[i].slowdown_count) + 1 : 0;
        const auto seconds_to_zero_errors = std::max(static_cast<time_t>(0), std::max(rounds_to_zero_errors, rounds_to_zero_slowdowns) * decrease_error_period - since_last_error_decrease);

        result.emplace_back(NestedPoolStatus{
            pools[i],
            updated_states[i].error_count,
            updated_states[i].slowdown_count,
            std::chrono::seconds{seconds_to_zero_errors}
        });
    }

    return result;
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getMany(const ConnectionTimeouts & timeouts,
                                                                        const Settings * settings,
                                                                        PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings);
    };

    std::vector<TryResult> results = getManyImpl(settings, pool_mode, try_get_entry);

    std::vector<Entry> entries;
    entries.reserve(results.size());
    for (auto & result : results)
        entries.emplace_back(std::move(result.entry));
    return entries;
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyForTableFunction(
    const ConnectionTimeouts & timeouts,
    const Settings * settings,
    PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings);
    };

    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyChecked(
    const ConnectionTimeouts & timeouts,
    const Settings * settings, PoolMode pool_mode,
    const QualifiedTableName & table_to_check)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings, &table_to_check);
    };

    return getManyImpl(settings, pool_mode, try_get_entry);
}

ConnectionPoolWithFailover::Base::GetPriorityFunc ConnectionPoolWithFailover::makeGetPriorityFunc(const Settings * settings)
{
    size_t offset = 0;
    LoadBalancing load_balancing = get_priority_load_balancing.load_balancing;
    if (settings)
    {
        offset = settings->load_balancing_first_offset % nested_pools.size();
        load_balancing = LoadBalancing(settings->load_balancing);
    }

    return get_priority_load_balancing.getPriorityFunc(load_balancing, offset, nested_pools.size());
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyImpl(
        const Settings * settings,
        PoolMode pool_mode,
        const TryGetEntryFunc & try_get_entry)
{
    if (nested_pools.empty())
        throw DB::Exception(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot get connection from ConnectionPoolWithFailover cause nested pools are empty");

    size_t min_entries = (settings && settings->skip_unavailable_shards) ? 0 : 1;
    size_t max_tries = (settings ?
        size_t{settings->connections_with_failover_max_tries} :
        size_t{DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES});
    size_t max_entries;
    if (pool_mode == PoolMode::GET_ALL)
    {
        min_entries = nested_pools.size();
        max_entries = nested_pools.size();
    }
    else if (pool_mode == PoolMode::GET_ONE)
        max_entries = 1;
    else if (pool_mode == PoolMode::GET_MANY)
        max_entries = settings ? size_t(settings->max_parallel_replicas) : 1;
    else
        throw DB::Exception("Unknown pool allocation mode", DB::ErrorCodes::LOGICAL_ERROR);

    GetPriorityFunc get_priority = makeGetPriorityFunc(settings);

    UInt64 max_ignored_errors = settings ? settings->distributed_replica_max_ignored_errors.value : 0;
    bool fallback_to_stale_replicas = settings ? settings->fallback_to_stale_replicas_for_distributed_queries.value : true;

    return Base::getMany(min_entries, max_entries, max_tries,
        max_ignored_errors, fallback_to_stale_replicas,
        try_get_entry, get_priority);
}

ConnectionPoolWithFailover::TryResult
ConnectionPoolWithFailover::tryGetEntry(
        IConnectionPool & pool,
        const ConnectionTimeouts & timeouts,
        std::string & fail_message,
        const Settings * settings,
        const QualifiedTableName * table_to_check)
{
    ConnectionEstablisher connection_establisher(&pool, &timeouts, settings, log, table_to_check);
    TryResult result;
    connection_establisher.run(result, fail_message);
    return result;
}

std::vector<ConnectionPoolWithFailover::Base::ShuffledPool> ConnectionPoolWithFailover::getShuffledPools(const Settings * settings)
{
    GetPriorityFunc get_priority = makeGetPriorityFunc(settings);
    UInt64 max_ignored_errors = settings ? settings->distributed_replica_max_ignored_errors.value : 0;
    return Base::getShuffledPools(max_ignored_errors, get_priority);
}

}
