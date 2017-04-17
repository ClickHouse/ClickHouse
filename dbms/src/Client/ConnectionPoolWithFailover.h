#pragma once

#include <Common/PoolWithFailoverBase.h>
#include <Client/ConnectionPool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int LOGICAL_ERROR;
}

/** Connection pool with fault tolerance.
  * Initialized by several other IConnectionPools.
  * When a connection is received, it tries to create or select a live connection from a pool,
  *  fetch them in some order, using no more than the specified number of attempts.
  * Pools with fewer errors are preferred;
  *  pools with the same number of errors are tried in random order.
  *
  * Note: if one of the nested pools is blocked due to overflow, then this pool will also be blocked.
  */
class ConnectionPoolWithFailover : public IConnectionPool, private PoolWithFailoverBase<IConnectionPool>
{
public:
    ConnectionPoolWithFailover(
            ConnectionPools & nested_pools_,
            LoadBalancing load_balancing,
            size_t max_tries_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
            time_t decrease_error_period_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD);

    using Entry = IConnectionPool::Entry;

    std::vector<Entry> getManyChecked(
            const Settings * settings, PoolMode pool_mode, const QualifiedTableName & table_to_check);

private:
    using Base = PoolWithFailoverBase<IConnectionPool>;

    /** Allocates connection to work. */
    Entry doGet(const Settings * settings) override; /// From IConnectionPool

    /** Allocates up to the specified number of connections to work.
      * Connections provide access to different replicas of one shard.
      */
    std::vector<Entry> doGetMany(const Settings * settings, PoolMode pool_mode) override; /// From IConnectionPool


    std::vector<Entry> getManyImpl(
            const Settings * settings,
            PoolMode pool_mode,
            const Base::TryGetEntryFunc & try_get_entry);

    Base::TryResult tryGetEntry(
            IConnectionPool & pool,
            std::string & fail_message,
            const Settings * settings,
            const QualifiedTableName * table_to_check = nullptr);

private:
    std::vector<size_t> hostname_differences; /// Distances from name of this host to the names of hosts of pools.
    LoadBalancing default_load_balancing;
};


}
