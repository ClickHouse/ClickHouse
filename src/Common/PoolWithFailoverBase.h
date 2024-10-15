#pragma once

#include <ctime>
#include <cstdlib>
#include <climits>
#include <random>
#include <functional>
#include <base/types.h>
#include <base/scope_guard.h>
#include <base/sort.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/Priority.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int LOGICAL_ERROR;
}
}

namespace ProfileEvents
{
    extern const Event DistributedConnectionFailAtAll;
    extern const Event DistributedConnectionSkipReadOnlyReplica;
}

/// This class provides a pool with fault tolerance. It is used for pooling of connections to replicated DB.
/// Initialized by several PoolBase objects.
/// When a connection is requested, tries to create or choose an alive connection from one of the nested pools.
/// Pools are tried in the order consistent with lexicographical order of (error count, slowdown count, config priority, priority, random number) tuples.
/// Number of tries for a single pool is limited by max_tries parameter.
/// The client can set nested pool priority by passing a GetPriority functor.
///
/// NOTE: if one of the nested pools blocks because it is empty, this pool will also block.
///
/// The client must provide a TryGetEntryFunc functor, which should perform a single try to get a connection from a nested pool.
/// This functor can also check if the connection satisfies some eligibility criterion (e.g. check if
/// the replica is up-to-date).

template <typename TNestedPool>
class PoolWithFailoverBase : private boost::noncopyable
{
public:
    using NestedPool = TNestedPool;
    using NestedPoolPtr = std::shared_ptr<NestedPool>;
    using Entry = typename NestedPool::Entry;
    using NestedPools = std::vector<NestedPoolPtr>;

    PoolWithFailoverBase(
            NestedPools nested_pools_,
            time_t decrease_error_period_,
            size_t max_error_cap_,
            LoggerPtr log_)
        : nested_pools(std::move(nested_pools_))
        , decrease_error_period(decrease_error_period_)
        , max_error_cap(max_error_cap_)
        , shared_pool_states(nested_pools.size())
        , log(log_)
    {
        for (size_t i = 0;i < nested_pools.size(); ++i)
            shared_pool_states[i].config_priority = nested_pools[i]->getConfigPriority();
    }

    struct TryResult
    {
        TryResult() = default;

        void reset() { *this = {}; }

        Entry entry; /// use isNull() to check if connection is established
        bool is_usable = false; /// if connection is established, then can be false only with table check
                                /// if table is not present on remote peer, -> it'll be false
        bool is_up_to_date = false; /// If true, the entry is a connection to up-to-date replica
                                    /// Depends on max_replica_delay_for_distributed_queries setting
        UInt32 delay = 0; /// Helps choosing the "least stale" option when all replicas are stale.
        bool is_readonly = false;   /// Table is in read-only mode, INSERT can ignore such replicas.
    };

    struct PoolState;

    using PoolStates = std::vector<PoolState>;

    struct ShuffledPool
    {
        NestedPoolPtr pool{};
        const PoolState * state{}; // WARNING: valid only during initial ordering, dangling
        size_t index = 0;
        size_t error_count = 0;
        size_t slowdown_count = 0;
    };

    /// This functor must be provided by a client. It must perform a single try that takes a connection
    /// from the provided pool and checks that it is good.
    using TryGetEntryFunc = std::function<TryResult(const NestedPoolPtr & pool, std::string & fail_message)>;

    /// The client can provide this functor to affect load balancing - the index of a pool is passed to
    /// this functor. The pools with lower result value will be tried first.
    using GetPriorityFunc = std::function<Priority(size_t index)>;

    /// Returns at least min_entries and at most max_entries connections (at most one connection per nested pool).
    /// The method will throw if it is unable to get min_entries alive connections or
    /// if fallback_to_stale_replicas is false and it is unable to get min_entries connections to up-to-date replicas.
    std::vector<TryResult> getMany(
            size_t min_entries, size_t max_entries, size_t max_tries,
            size_t max_ignored_errors,
            bool fallback_to_stale_replicas,
            bool skip_read_only_replicas,
            const TryGetEntryFunc & try_get_entry,
            const GetPriorityFunc & get_priority);

    // Returns if the TryResult provided is an invalid one that cannot be used. Used to prevent logical errors.
    bool isTryResultInvalid(const TryResult & result, bool skip_read_only_replicas) const
    {
        return result.entry.isNull() || !result.is_usable || (skip_read_only_replicas && result.is_readonly);
    }

    TryResult getValidTryResult(const std::vector<TryResult> & results, bool skip_read_only_replicas) const
    {
        if (results.empty())
            throw DB::Exception(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot get any valid connection because all connection tries failed");

        auto result = results.front();
        if (isTryResultInvalid(result, skip_read_only_replicas))
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                "Got an invalid connection result: entry.isNull {}, is_usable {}, is_up_to_date {}, delay {}, is_readonly {}, skip_read_only_replicas {}",
                result.entry.isNull(), result.is_usable, result.is_up_to_date, result.delay, result.is_readonly, skip_read_only_replicas);

        return result;
    }

    size_t getPoolSize() const { return nested_pools.size(); }

protected:

    /// Returns a single connection.
    Entry get(size_t max_ignored_errors, bool fallback_to_stale_replicas,
        const TryGetEntryFunc & try_get_entry, const GetPriorityFunc & get_priority = GetPriorityFunc());

    /// This function returns a copy of pool states to avoid race conditions when modifying shared pool states.
    PoolStates updatePoolStates(size_t max_ignored_errors);

    void updateErrorCounts(PoolStates & states, time_t & last_decrease_time) const;

    std::vector<ShuffledPool> getShuffledPools(size_t max_ignored_errors, const GetPriorityFunc & get_priority, bool use_slowdown_count = false);

    inline void updateSharedErrorCounts(std::vector<ShuffledPool> & shuffled_pools);

    auto getPoolExtendedStates() const
    {
        std::lock_guard lock(pool_states_mutex);
        return std::make_tuple(shared_pool_states, nested_pools, last_error_decrease_time);
    }

    const NestedPools nested_pools;

    const time_t decrease_error_period;
    const size_t max_error_cap;

    mutable std::mutex pool_states_mutex;
    PoolStates shared_pool_states;
    /// The time when error counts were last decreased.
    time_t last_error_decrease_time = 0;

    LoggerPtr log;
};


template <typename TNestedPool>
std::vector<typename PoolWithFailoverBase<TNestedPool>::ShuffledPool>
PoolWithFailoverBase<TNestedPool>::getShuffledPools(
    size_t max_ignored_errors, const PoolWithFailoverBase::GetPriorityFunc & get_priority, bool use_slowdown_count)
{
    /// Update random numbers and error counts.
    PoolStates pool_states = updatePoolStates(max_ignored_errors);
    if (get_priority)
    {
        for (size_t i = 0; i < pool_states.size(); ++i)
            pool_states[i].priority = get_priority(i);
    }

    /// Sort the pools into order in which they will be tried (based on respective PoolStates).
    /// Note that `error_count` and `slowdown_count` are used for ordering, but set to zero in the resulting ShuffledPool
    std::vector<ShuffledPool> shuffled_pools;
    shuffled_pools.reserve(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
        shuffled_pools.emplace_back(ShuffledPool{.pool = nested_pools[i], .state = &pool_states[i], .index = i});

    ::sort(
        shuffled_pools.begin(), shuffled_pools.end(),
        [use_slowdown_count](const ShuffledPool & lhs, const ShuffledPool & rhs)
        {
            return PoolState::compare(*lhs.state, *rhs.state, use_slowdown_count);
        });

    return shuffled_pools;
}

template <typename TNestedPool>
inline void PoolWithFailoverBase<TNestedPool>::updateSharedErrorCounts(std::vector<ShuffledPool> & shuffled_pools)
{
    std::lock_guard lock(pool_states_mutex);
    for (const ShuffledPool & pool: shuffled_pools)
    {
        auto & pool_state = shared_pool_states[pool.index];
        pool_state.error_count = std::min<UInt64>(max_error_cap, pool_state.error_count + pool.error_count);
        pool_state.slowdown_count += pool.slowdown_count;
    }
}

template <typename TNestedPool>
typename TNestedPool::Entry
PoolWithFailoverBase<TNestedPool>::get(size_t max_ignored_errors, bool fallback_to_stale_replicas,
    const TryGetEntryFunc & try_get_entry, const GetPriorityFunc & get_priority)
{
    std::vector<TryResult> results = getMany(
        /* min_entries= */ 1,
        /* max_entries= */ 1,
        /* max_tries= */ 1,
        max_ignored_errors,
        fallback_to_stale_replicas,
        /* skip_read_only_replicas= */ false,
        try_get_entry, get_priority);
    if (results.empty() || results[0].entry.isNull())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                "PoolWithFailoverBase::getMany() returned less than min_entries entries.");
    return results[0].entry;
}

template <typename TNestedPool>
std::vector<typename PoolWithFailoverBase<TNestedPool>::TryResult>
PoolWithFailoverBase<TNestedPool>::getMany(
        size_t min_entries, size_t max_entries, size_t max_tries,
        size_t max_ignored_errors,
        bool fallback_to_stale_replicas,
        bool skip_read_only_replicas,
        const TryGetEntryFunc & try_get_entry,
        const GetPriorityFunc & get_priority)
{
    std::vector<ShuffledPool> shuffled_pools = getShuffledPools(max_ignored_errors, get_priority);

    /// Limit `max_tries` value by `max_error_cap` to avoid unlimited number of retries
    max_tries = std::min(max_tries, max_error_cap);

    /// We will try to get a connection from each pool until a connection is produced or max_tries is reached.
    std::vector<TryResult> try_results(shuffled_pools.size());
    size_t entries_count = 0;
    size_t usable_count = 0;
    size_t up_to_date_count = 0;
    size_t failed_pools_count = 0;

    /// At exit update shared error counts with error counts occurred during this call.
    SCOPE_EXIT(
    {
        updateSharedErrorCounts(shuffled_pools);
    });

    std::string fail_messages;
    bool finished = false;
    while (!finished)
    {
        for (size_t i = 0; i < shuffled_pools.size(); ++i)
        {
            if (up_to_date_count >= max_entries /// Already enough good entries.
                || entries_count + failed_pools_count >= nested_pools.size()) /// No more good entries will be produced.
            {
                finished = true;
                break;
            }

            ShuffledPool & shuffled_pool = shuffled_pools[i];
            TryResult & result = try_results[i];
            if (max_tries && (shuffled_pool.error_count >= max_tries || !result.entry.isNull()))
                continue;

            std::string fail_message;
            result = try_get_entry(shuffled_pool.pool, fail_message);

            if (!fail_message.empty())
                fail_messages += fail_message + '\n';

            if (!result.entry.isNull())
            {
                ++entries_count;
                if (result.is_usable)
                {
                    if (skip_read_only_replicas && result.is_readonly)
                        ProfileEvents::increment(ProfileEvents::DistributedConnectionSkipReadOnlyReplica);
                    else
                    {
                        ++usable_count;
                        if (result.is_up_to_date)
                            ++up_to_date_count;
                    }
                }
            }
            else
            {
                LOG_WARNING(log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), fail_message);

                shuffled_pool.error_count = std::min(max_error_cap, shuffled_pool.error_count + 1);

                if (shuffled_pool.error_count >= max_tries)
                {
                    ++failed_pools_count;
                    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
                }
            }
        }
    }

    if (usable_count < min_entries)
        throw DB::NetException(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
                "All connection tries failed. Log: \n\n{}\n", fail_messages);

    std::erase_if(try_results, [&](const TryResult & r) { return isTryResultInvalid(r, skip_read_only_replicas); });

    /// Sort so that preferred items are near the beginning.
    std::stable_sort(
            try_results.begin(), try_results.end(),
            [](const TryResult & left, const TryResult & right)
            {
                return std::forward_as_tuple(!left.is_up_to_date, left.delay)
                    < std::forward_as_tuple(!right.is_up_to_date, right.delay);
            });

    if (fallback_to_stale_replicas)
    {
        /// There is not enough up-to-date entries but we are allowed to return stale entries.
        /// Gather all up-to-date ones and least-bad stale ones.

        size_t size = std::min(try_results.size(), max_entries);
        try_results.resize(size);
    }
    else if (up_to_date_count >= min_entries)
    {
        if (try_results.size() < up_to_date_count)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Could not find enough connections for up-to-date results. Got: {}, needed: {}", try_results.size(), up_to_date_count);

        /// There is enough up-to-date entries.
        try_results.resize(up_to_date_count);
    }
    else
        throw DB::Exception(DB::ErrorCodes::ALL_REPLICAS_ARE_STALE,
                "Could not find enough connections to up-to-date replicas. Got: {}, needed: {}", up_to_date_count, max_entries);

    return try_results;
}

template <typename TNestedPool>
struct PoolWithFailoverBase<TNestedPool>::PoolState
{
    UInt64 error_count = 0;
    /// The number of slowdowns that led to changing replica in HedgedRequestsFactory
    UInt64 slowdown_count = 0;
    /// Priority from the <remote_server> configuration.
    Priority config_priority{1};
    /// Priority from the GetPriorityFunc.
    Priority priority{0};
    UInt64 random = 0;

    void randomize()
    {
        random = rng();
    }

    static bool compare(const PoolState & lhs, const PoolState & rhs, bool use_slowdown_count)
    {
        if (use_slowdown_count)
            return std::forward_as_tuple(lhs.error_count, lhs.slowdown_count, lhs.config_priority, lhs.priority, lhs.random)
                < std::forward_as_tuple(rhs.error_count, rhs.slowdown_count, rhs.config_priority, rhs.priority, rhs.random);
        return std::forward_as_tuple(lhs.error_count, lhs.config_priority, lhs.priority, lhs.random)
            < std::forward_as_tuple(rhs.error_count, rhs.config_priority, rhs.priority, rhs.random);
    }

private:
    std::minstd_rand rng = std::minstd_rand(static_cast<uint_fast32_t>(randomSeed()));
};

template <typename TNestedPool>
typename PoolWithFailoverBase<TNestedPool>::PoolStates
PoolWithFailoverBase<TNestedPool>::updatePoolStates(size_t max_ignored_errors)
{
    PoolStates result;
    result.reserve(nested_pools.size());

    {
        std::lock_guard lock(pool_states_mutex);

        for (auto & state : shared_pool_states)
            state.randomize();

        updateErrorCounts(shared_pool_states, last_error_decrease_time);
        result.assign(shared_pool_states.begin(), shared_pool_states.end());
    }

    /// distributed_replica_max_ignored_errors
    for (auto & state : result)
        state.error_count = state.error_count > max_ignored_errors ? state.error_count - max_ignored_errors : 0;

    return result;
}

template <typename TNestedPool>
void PoolWithFailoverBase<TNestedPool>::updateErrorCounts(PoolWithFailoverBase<TNestedPool>::PoolStates & states, time_t & last_decrease_time) const
{
    time_t current_time = time(nullptr);

    if (last_decrease_time)
    {
        time_t delta = current_time - last_decrease_time;

        if (delta >= 0)
        {
            const UInt64 max_bits = sizeof(UInt64) * CHAR_BIT;
            size_t shift_amount = max_bits;
            /// Divide error counts by 2 every decrease_error_period seconds.
            if (decrease_error_period)
                shift_amount = delta / decrease_error_period;
            /// Update time but don't do it more often than once a period.
            /// Else if the function is called often enough, error count will never decrease.
            if (shift_amount)
                last_decrease_time = current_time;

            if (shift_amount >= max_bits)
            {
                for (auto & state : states)
                {
                    state.error_count = 0;
                    state.slowdown_count = 0;
                }
            }
            else if (shift_amount)
            {
                for (auto & state : states)
                {
                    state.error_count >>= shift_amount;
                    state.slowdown_count >>= shift_amount;
                }
            }
        }
    }
    else
        last_decrease_time = current_time;
}
