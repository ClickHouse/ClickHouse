#include <Client/ConnectionPoolWithFailover.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int LOGICAL_ERROR;
}


ConnectionPoolWithFailover::ConnectionPoolWithFailover(
        ConnectionPoolPtrs nested_pools_,
        LoadBalancing load_balancing,
        size_t max_tries_,
        time_t decrease_error_period_)
    : Base(std::move(nested_pools_), max_tries_, decrease_error_period_, &Logger::get("ConnectionPoolWithFailover"))
    , default_load_balancing(load_balancing)
{
    const std::string & local_hostname = getFQDNOrHostName();

    hostname_differences.resize(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i]);
        hostname_differences[i] = getHostNameDifference(local_hostname, connection_pool.getHost());
    }
}

IConnectionPool::Entry ConnectionPoolWithFailover::get(const Settings * settings, bool /*force_connected*/)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings);
    };

    GetPriorityFunc get_priority;
    switch (settings ? LoadBalancing(settings->load_balancing) : default_load_balancing)
    {
    case LoadBalancing::NEAREST_HOSTNAME:
        get_priority = [&](size_t i) { return hostname_differences[i]; };
        break;
    case LoadBalancing::IN_ORDER:
        get_priority = [](size_t i) { return i; };
        break;
    case LoadBalancing::RANDOM:
        break;
    case LoadBalancing::FIRST_OR_RANDOM:
        get_priority = [](size_t i) -> size_t { return i >= 1; };
        break;
    }

    return Base::get(try_get_entry, get_priority);
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getMany(const Settings * settings, PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings);
    };

    std::vector<TryResult> results = getManyImpl(settings, pool_mode, try_get_entry);

    std::vector<Entry> entries;
    entries.reserve(results.size());
    for (auto & result : results)
        entries.emplace_back(std::move(result.entry));
    return entries;
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyForTableFunction(const Settings * settings, PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings);
    };

    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyChecked(
        const Settings * settings, PoolMode pool_mode, const QualifiedTableName & table_to_check)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings, &table_to_check);
    };

    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyImpl(
        const Settings * settings,
        PoolMode pool_mode,
        const TryGetEntryFunc & try_get_entry)
{
    size_t min_entries = (settings && settings->skip_unavailable_shards) ? 0 : 1;
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

    GetPriorityFunc get_priority;
    switch (settings ? LoadBalancing(settings->load_balancing) : default_load_balancing)
    {
    case LoadBalancing::NEAREST_HOSTNAME:
        get_priority = [&](size_t i) { return hostname_differences[i]; };
        break;
    case LoadBalancing::IN_ORDER:
        get_priority = [](size_t i) { return i; };
        break;
    case LoadBalancing::RANDOM:
        break;
    case LoadBalancing::FIRST_OR_RANDOM:
        get_priority = [](size_t i) -> size_t { return i >= 1; };
        break;
    }

    bool fallback_to_stale_replicas = settings ? bool(settings->fallback_to_stale_replicas_for_distributed_queries) : true;

    return Base::getMany(min_entries, max_entries, try_get_entry, get_priority, fallback_to_stale_replicas);
}

ConnectionPoolWithFailover::TryResult
ConnectionPoolWithFailover::tryGetEntry(
        IConnectionPool & pool,
        std::string & fail_message,
        const Settings * settings,
        const QualifiedTableName * table_to_check)
{
    TryResult result;
    try
    {
        result.entry = pool.get(settings, /* force_connected = */ false);

        UInt64 server_revision = 0;
        if (table_to_check)
            server_revision = result.entry->getServerRevision();

        if (!table_to_check || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        {
            result.entry->forceConnected();
            result.is_usable = true;
            result.is_up_to_date = true;
            return result;
        }

        /// Only status of the remote table corresponding to the Distributed table is taken into account.
        /// TODO: request status for joined tables also.
        TablesStatusRequest status_request;
        status_request.tables.emplace(*table_to_check);

        TablesStatusResponse status_response = result.entry->getTablesStatus(status_request);
        auto table_status_it = status_response.table_states_by_id.find(*table_to_check);
        if (table_status_it == status_response.table_states_by_id.end())
        {
            fail_message = "There is no table " + table_to_check->database + "." + table_to_check->table
                + " on server: " + result.entry->getDescription();
            LOG_WARNING(log, fail_message);
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);

            return result;
        }

        result.is_usable = true;

        UInt64 max_allowed_delay = settings ? UInt64(settings->max_replica_delay_for_distributed_queries) : 0;
        if (!max_allowed_delay)
        {
            result.is_up_to_date = true;
            return result;
        }

        UInt32 delay = table_status_it->second.absolute_delay;

        if (delay < max_allowed_delay)
            result.is_up_to_date = true;
        else
        {
            result.is_up_to_date = false;
            result.staleness = delay;

            LOG_TRACE(
                    log, "Server " << result.entry->getDescription() << " has unacceptable replica delay "
                    << "for table " << table_to_check->database << "." << table_to_check->table
                    << ": " << delay);
            ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw;

        fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);

        if (!result.entry.isNull())
        {
            result.entry->disconnect();
            result.reset();
        }
    }
    return result;
}

}
