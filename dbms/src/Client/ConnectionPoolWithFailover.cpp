#include <Client/ConnectionPoolWithFailover.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Settings.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

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
        const std::string & host = connection_pool.getHost();

        size_t hostname_difference = 0;
        for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
            if (local_hostname[i] != host[i])
                ++hostname_difference;

        hostname_differences[i] = hostname_difference;
    }
}

IConnectionPool::Entry ConnectionPoolWithFailover::get(const Settings * settings)
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
    }

    return Base::get(try_get_entry, get_priority);
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getMany(const Settings * settings, PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings);
    };
    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getManyChecked(
        const Settings * settings, PoolMode pool_mode, const QualifiedTableName & table_to_check)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message)
    {
        return tryGetEntry(pool, fail_message, settings, &table_to_check);
    };
    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPool::Entry> ConnectionPoolWithFailover::getManyImpl(
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
        result.entry = pool.get(settings);

        UInt64 max_allowed_delay = settings ? UInt64(settings->max_replica_delay_for_distributed_queries) : 0;

        String server_name;
        UInt64 server_version_major;
        UInt64 server_version_minor;
        UInt64 server_revision;
        if (table_to_check && max_allowed_delay)
            result.entry->getServerVersion(server_name, server_version_major, server_version_minor, server_revision);

        if (!table_to_check || !max_allowed_delay || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        {
            result.entry->forceConnected();
            result.is_up_to_date = true;
            return result;
        }

        /// Only the delay of the remote table of the corresponding Distributed table is taken into account.
        /// TODO: calculate delay for joined tables also.
        TablesStatusRequest status_request;
        status_request.tables = { *table_to_check };

        auto status_response = result.entry->getTablesStatus(status_request);
        if (status_response.table_states_by_id.size() != status_request.tables.size())
            throw Exception(
                    "Bad TablesStatus response (from " + result.entry->getDescription() + ")",
                    ErrorCodes::LOGICAL_ERROR);

        UInt32 max_delay = 0;
        for (const auto & kv: status_response.table_states_by_id)
        {
            const TableStatus & status = kv.second;

            if (status.is_replicated)
                max_delay = std::max(max_delay, status.absolute_delay);
        }

        if (max_delay < max_allowed_delay)
            result.is_up_to_date = true;
        else
        {
            result.is_up_to_date = false;
            result.staleness = max_delay;

            LOG_TRACE(
                    log, "Connection " << result.entry->getDescription() << " has unacceptable replica delay "
                    << "for table " << table_to_check->database << "." << table_to_check->table
                    << ": "  << max_delay);
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
            result.entry = Entry();
        }
    }
    return result;
};

}
