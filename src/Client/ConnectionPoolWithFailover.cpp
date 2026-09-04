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
namespace Setting
{
    extern const SettingsUInt64 connections_with_failover_max_tries;
    extern const SettingsBool distributed_insert_skip_read_only_replicas;
    extern const SettingsUInt64 distributed_replica_max_ignored_errors;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsUInt64 load_balancing_first_offset;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool skip_unavailable_shards;
}

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
    : Base(std::move(nested_pools_), decrease_error_period_, max_error_cap_, getLogger("ConnectionPoolWithFailover"))
    , get_priority_load_balancing(load_balancing)
{
    const std::string & local_hostname = getFQDNOrHostName();

    get_priority_load_balancing.hostname_prefix_distance.resize(nested_pools.size());
    get_priority_load_balancing.hostname_levenshtein_distance.resize(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i]);
        get_priority_load_balancing.hostname_prefix_distance[i] = getHostNamePrefixDistance(local_hostname, connection_pool.getHost());
        get_priority_load_balancing.hostname_levenshtein_distance[i] = getHostNameLevenshteinDistance(local_hostname, connection_pool.getHost());
    }
}

IConnectionPool::Entry ConnectionPoolWithFailover::get(const ConnectionTimeouts & timeouts)
{
    Settings settings;
    settings[Setting::load_balancing] = get_priority_load_balancing.load_balancing;
    settings[Setting::load_balancing_first_offset] = 0;
    settings[Setting::distributed_replica_max_ignored_errors] = 0;
    settings[Setting::fallback_to_stale_replicas_for_distributed_queries] = true;

    return get(timeouts, settings, /* force_connected= */ true);
}

IConnectionPool::Entry ConnectionPoolWithFailover::get(const ConnectionTimeouts & timeouts,
                                                       const Settings & settings,
                                                       bool /*force_connected*/)
{
    if (nested_pools.empty())
        throw DB::Exception(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
                            "Cannot get connection from ConnectionPoolWithFailover cause nested pools are empty");

    TryGetEntryFunc try_get_entry = [&](const NestedPoolPtr & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings);
    };

    const size_t offset = settings[Setting::load_balancing_first_offset] % nested_pools.size();

    GetPriorityFunc get_priority = get_priority_load_balancing.getPriorityFunc(settings[Setting::load_balancing], offset, nested_pools.size());

    const UInt64 max_ignored_errors = settings[Setting::distributed_replica_max_ignored_errors];
    const bool fallback_to_stale_replicas = settings[Setting::fallback_to_stale_replicas_for_distributed_queries];

    return Base::get(max_ignored_errors, fallback_to_stale_replicas, try_get_entry, get_priority);
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

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getMany(
    const ConnectionTimeouts & timeouts,
    const Settings & settings,
    PoolMode pool_mode,
    AsyncCallback async_callback,
    std::optional<bool> skip_unavailable_endpoints,
    GetPriorityForLoadBalancing::Func priority_func)
{
    bool force_connected = skip_unavailable_endpoints.value_or(false);
    TryGetEntryFunc try_get_entry = [&](const NestedPoolPtr & pool, std::string & fail_message)
    { return tryGetEntry(pool, timeouts, fail_message, settings, nullptr, async_callback, force_connected); };

    std::vector<TryResult> results = getManyImpl(settings, pool_mode, try_get_entry, skip_unavailable_endpoints, priority_func);

    std::vector<Entry> entries;
    entries.reserve(results.size());
    for (auto & result : results)
        entries.emplace_back(std::move(result.entry));
    return entries;
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyForTableFunction(
    const ConnectionTimeouts & timeouts,
    const Settings & settings,
    PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](const NestedPoolPtr & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, timeouts, fail_message, settings);
    };

    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyChecked(
    const ConnectionTimeouts & timeouts,
    const Settings & settings,
    PoolMode pool_mode,
    const QualifiedTableName & table_to_check,
    AsyncCallback async_callback,
    std::optional<bool> skip_unavailable_endpoints,
    GetPriorityForLoadBalancing::Func priority_func)
{
    TryGetEntryFunc try_get_entry = [&](const NestedPoolPtr & pool, std::string & fail_message)
    { return tryGetEntry(pool, timeouts, fail_message, settings, &table_to_check, async_callback); };

    return getManyImpl(settings, pool_mode, try_get_entry, skip_unavailable_endpoints, priority_func);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyCheckedForInsert(
    const ConnectionTimeouts & timeouts,
    const Settings & settings,
    PoolMode pool_mode,
    const QualifiedTableName & table_to_check)
{
    TryGetEntryFunc try_get_entry = [&](const NestedPoolPtr & pool, std::string & fail_message)
    { return tryGetEntry(pool, timeouts, fail_message, settings, &table_to_check, /*async_callback=*/ {}); };

    return getManyImpl(settings, pool_mode, try_get_entry,
        /*skip_unavailable_endpoints=*/ false, /// skip_unavailable_endpoints is used to get the min number of entries, and we need at least one
        /*priority_func=*/ {},
        settings[Setting::distributed_insert_skip_read_only_replicas]);
}

ConnectionPoolWithFailover::Base::GetPriorityFunc ConnectionPoolWithFailover::makeGetPriorityFunc(const Settings & settings)
{
    const size_t offset = settings[Setting::load_balancing_first_offset] % nested_pools.size();
    return get_priority_load_balancing.getPriorityFunc(settings[Setting::load_balancing], offset, nested_pools.size());
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyImpl(
    const Settings & settings,
    PoolMode pool_mode,
    const TryGetEntryFunc & try_get_entry,
    std::optional<bool> skip_unavailable_endpoints,
    GetPriorityForLoadBalancing::Func priority_func,
    bool skip_read_only_replicas)
{
    if (nested_pools.empty())
        throw DB::Exception(
            DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
            "Cannot get connection from ConnectionPoolWithFailover cause nested pools are empty");

    if (!skip_unavailable_endpoints.has_value())
        skip_unavailable_endpoints = settings[Setting::skip_unavailable_shards];

    size_t min_entries = skip_unavailable_endpoints.value() ? 0 : 1;

    size_t max_tries = settings[Setting::connections_with_failover_max_tries];
    size_t max_entries;
    if (pool_mode == PoolMode::GET_ALL)
    {
        min_entries = nested_pools.size();
        max_entries = nested_pools.size();
    }
    else if (pool_mode == PoolMode::GET_ONE)
    {
        max_entries = 1;
    }
    else if (pool_mode == PoolMode::GET_MANY)
    {
        max_entries = settings[Setting::max_parallel_replicas];
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown pool allocation mode");
    }

    if (!priority_func)
        priority_func = makeGetPriorityFunc(settings);

    UInt64 max_ignored_errors = settings[Setting::distributed_replica_max_ignored_errors].value;
    bool fallback_to_stale_replicas = settings[Setting::fallback_to_stale_replicas_for_distributed_queries].value;

    return Base::getMany(min_entries, max_entries, max_tries, max_ignored_errors, fallback_to_stale_replicas, skip_read_only_replicas, try_get_entry, priority_func);
}

ConnectionPoolWithFailover::TryResult
ConnectionPoolWithFailover::tryGetEntry(
        const ConnectionPoolPtr & pool,
        const ConnectionTimeouts & timeouts,
        std::string & fail_message,
        const Settings & settings,
        const QualifiedTableName * table_to_check,
        [[maybe_unused]] AsyncCallback async_callback,
        bool force_connected)
{
#if defined(OS_LINUX)
    if (async_callback)
    {
        ConnectionEstablisherAsync connection_establisher_async(pool, &timeouts, settings, log, table_to_check);
        while (true)
        {
            connection_establisher_async.resumeConnectionWithForceOption(force_connected);

            if (connection_establisher_async.isFinished())
                break;

            async_callback(
                connection_establisher_async.getFileDescriptor(),
                0,
                AsyncEventTimeoutType::NONE,
                "Connection establisher file descriptor",
                AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);
        }

        fail_message = connection_establisher_async.getFailMessage();
        return connection_establisher_async.getResult();
    }
#endif

    ConnectionEstablisher connection_establisher(pool, &timeouts, settings, log, table_to_check);
    TryResult result;
    connection_establisher.run(result, fail_message, force_connected);
    return result;
}

std::vector<ConnectionPoolWithFailover::Base::ShuffledPool>
ConnectionPoolWithFailover::getShuffledPools(const Settings & settings, GetPriorityForLoadBalancing::Func priority_func, bool use_slowdown_count)
{
    if (!priority_func)
        priority_func = makeGetPriorityFunc(settings);

    UInt64 max_ignored_errors = settings[Setting::distributed_replica_max_ignored_errors].value;
    return Base::getShuffledPools(max_ignored_errors, priority_func, use_slowdown_count);
}

}
