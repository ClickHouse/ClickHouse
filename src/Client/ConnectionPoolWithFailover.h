#pragma once

#include <Common/PoolWithFailoverBase.h>
#include <Common/GetPriorityForLoadBalancing.h>
#include <Client/ConnectionPool.h>

#include <chrono>
#include <vector>


namespace DB
{

/** Connection pool with fault tolerance.
  * Initialized by several other IConnectionPools.
  * When a connection is received, it tries to create or select a live connection from a pool,
  *  fetch them in some order, using no more than the specified number of attempts.
  * Pools with fewer errors are preferred;
  *  pools with the same number of errors are tried in random order.
  *
  * Note: if one of the nested pools is blocked due to overflow, then this pool will also be blocked.
  */

/// Specifies how many connections to return from ConnectionPoolWithFailover::getMany() method.
enum class PoolMode : uint8_t
{
    /// Return exactly one connection.
    GET_ONE = 0,
    /// Return a number of connections, this number being determined by max_parallel_replicas setting.
    GET_MANY,
    /// Return a connection from each nested pool.
    GET_ALL
};

class ConnectionPoolWithFailover : public IConnectionPool, private PoolWithFailoverBase<IConnectionPool>
{
public:
    ConnectionPoolWithFailover(
            ConnectionPoolPtrs nested_pools_,
            LoadBalancing load_balancing,
            time_t decrease_error_period_ = DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD,
            size_t max_error_cap = DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT);

    using Entry = IConnectionPool::Entry;
    using PoolWithFailoverBase<IConnectionPool>::getValidTryResult;

    /** Allocates connection to work. */
    Entry get(const ConnectionTimeouts & timeouts) override;
    Entry get(const ConnectionTimeouts & timeouts,
              const Settings & settings,
              bool force_connected) override; /// From IConnectionPool

    /** Allocates up to the specified number of connections to work.
      * Connections provide access to different replicas of one shard.
      */
    std::vector<Entry> getMany(
        const ConnectionTimeouts & timeouts,
        const Settings & settings,
        PoolMode pool_mode,
        AsyncCallback async_callback = {},
        std::optional<bool> skip_unavailable_endpoints = std::nullopt,
        GetPriorityForLoadBalancing::Func priority_func = {});

    /// The same as getMany(), but return std::vector<TryResult>.
    std::vector<TryResult> getManyForTableFunction(const ConnectionTimeouts & timeouts,
                                                   const Settings & settings, PoolMode pool_mode);

    using Base = PoolWithFailoverBase<IConnectionPool>;
    using TryResult = Base::TryResult;

    /// The same as getMany(), but check that replication delay for table_to_check is acceptable.
    /// Delay threshold is taken from settings.
    std::vector<TryResult> getManyChecked(
        const ConnectionTimeouts & timeouts,
        const Settings & settings,
        PoolMode pool_mode,
        const QualifiedTableName & table_to_check,
        AsyncCallback async_callback = {},
        std::optional<bool> skip_unavailable_endpoints = std::nullopt,
        GetPriorityForLoadBalancing::Func priority_func = {});
    /// The same as getManyChecked(), but respects distributed_insert_skip_read_only_replicas setting.
    std::vector<TryResult> getManyCheckedForInsert(
        const ConnectionTimeouts & timeouts,
        const Settings & settings,
        PoolMode pool_mode,
        const QualifiedTableName & table_to_check);

    struct NestedPoolStatus
    {
        const Base::NestedPoolPtr pool;
        size_t error_count = 0;
        size_t slowdown_count = 0;
        std::chrono::seconds estimated_recovery_time;
    };

    using Status = std::vector<NestedPoolStatus>;
    Status getStatus() const;

    std::vector<Base::ShuffledPool> getShuffledPools(const Settings & settings, GetPriorityFunc priority_func = {}, bool use_slowdown_count = false);

    size_t getMaxErrorCap() const { return Base::max_error_cap; }

    void updateSharedError(std::vector<ShuffledPool> & shuffled_pools)
    {
        Base::updateSharedErrorCounts(shuffled_pools);
    }

    size_t getPoolSize() const { return Base::getPoolSize(); }

private:
    /// Get the values of relevant settings and call Base::getMany()
    std::vector<TryResult> getManyImpl(
        const Settings & settings,
        PoolMode pool_mode,
        const TryGetEntryFunc & try_get_entry,
        std::optional<bool> skip_unavailable_endpoints = std::nullopt,
        GetPriorityForLoadBalancing::Func priority_func = {},
        bool skip_read_only_replicas = false);

    /// Try to get a connection from the pool and check that it is good.
    /// If table_to_check is not null and the check is enabled in settings, check that replication delay
    /// for this table is not too large.
    TryResult tryGetEntry(
            const ConnectionPoolPtr & pool,
            const ConnectionTimeouts & timeouts,
            std::string & fail_message,
            const Settings & settings,
            const QualifiedTableName * table_to_check = nullptr,
            AsyncCallback async_callback = {});

    GetPriorityForLoadBalancing::Func makeGetPriorityFunc(const Settings & settings);

    GetPriorityForLoadBalancing get_priority_load_balancing;
};

using ConnectionPoolWithFailoverPtr = std::shared_ptr<ConnectionPoolWithFailover>;
using ConnectionPoolWithFailoverPtrs = std::vector<ConnectionPoolWithFailoverPtr>;

}
