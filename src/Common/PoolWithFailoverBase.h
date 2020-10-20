#pragma once

#include <time.h>
#include <cstdlib>
#include <climits>
#include <random>
#include <functional>
#include <common/types.h>
#include <ext/scope_guard.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>


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
    extern const Event DistributedConnectionFailTry;
    extern const Event DistributedConnectionFailAtAll;
}

/// This class provides a pool with fault tolerance. It is used for pooling of connections to replicated DB.
/// Initialized by several PoolBase objects.
/// When a connection is requested, tries to create or choose an alive connection from one of the nested pools.
/// Pools are tried in the order consistent with lexicographical order of (error count, priority, random number) tuples.
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
            Poco::Logger * log_)
        : nested_pools(std::move(nested_pools_))
        , decrease_error_period(decrease_error_period_)
        , max_error_cap(max_error_cap_)
        , shared_pool_states(nested_pools.size())
        , log(log_)
    {
        for (size_t i = 0;i < nested_pools.size(); ++i)
            shared_pool_states[i].config_priority = nested_pools[i]->getPriority();
    }

    struct TryResult
    {
        TryResult() = default;

        explicit TryResult(Entry entry_)
            : entry(std::move(entry_))
            , is_usable(true)
            , is_up_to_date(true)
        {
        }

        void reset()
        {
            entry = Entry();
            is_usable = false;
            is_up_to_date = false;
            staleness = 0.0;
        }

        Entry entry;
        bool is_usable = false; /// If false, the entry is unusable for current request
                                /// (but may be usable for other requests, so error counts are not incremented)
        bool is_up_to_date = false; /// If true, the entry is a connection to up-to-date replica.
        double staleness = 0.0; /// Helps choosing the "least stale" option when all replicas are stale.
    };

    /// This functor must be provided by a client. It must perform a single try that takes a connection
    /// from the provided pool and checks that it is good.
    using TryGetEntryFunc = std::function<TryResult(NestedPool & pool, std::string & fail_message)>;

    /// The client can provide this functor to affect load balancing - the index of a pool is passed to
    /// this functor. The pools with lower result value will be tried first.
    using GetPriorityFunc = std::function<size_t(size_t index)>;


    /// Returns at least min_entries and at most max_entries connections (at most one connection per nested pool).
    /// The method will throw if it is unable to get min_entries alive connections or
    /// if fallback_to_stale_replicas is false and it is unable to get min_entries connections to up-to-date replicas.
    std::vector<TryResult> getMany(
            size_t min_entries, size_t max_entries, size_t max_tries,
            size_t max_ignored_errors,
            bool fallback_to_stale_replicas,
            const TryGetEntryFunc & try_get_entry,
            const GetPriorityFunc & get_priority = GetPriorityFunc());

protected:
    struct PoolState;

    using PoolStates = std::vector<PoolState>;

    /// Returns a single connection.
    Entry get(size_t max_ignored_errors, bool fallback_to_stale_replicas,
        const TryGetEntryFunc & try_get_entry, const GetPriorityFunc & get_priority = GetPriorityFunc());

    /// This function returns a copy of pool states to avoid race conditions when modifying shared pool states.
    PoolStates updatePoolStates(size_t max_ignored_errors);

    auto getPoolExtendedStates() const
    {
        std::lock_guard lock(pool_states_mutex);
        return std::make_tuple(shared_pool_states, nested_pools, last_error_decrease_time);
    }

    NestedPools nested_pools;

    const time_t decrease_error_period;
    const size_t max_error_cap;

    mutable std::mutex pool_states_mutex;
    PoolStates shared_pool_states;
    /// The time when error counts were last decreased.
    time_t last_error_decrease_time = 0;

    Poco::Logger * log;
};

template <typename TNestedPool>
typename TNestedPool::Entry
PoolWithFailoverBase<TNestedPool>::get(size_t max_ignored_errors, bool fallback_to_stale_replicas,
    const TryGetEntryFunc & try_get_entry, const GetPriorityFunc & get_priority)
{
    std::vector<TryResult> results = getMany(
        1 /* min entries */, 1 /* max entries */, 1 /* max tries */,
        max_ignored_errors, fallback_to_stale_replicas,
        try_get_entry, get_priority);
    if (results.empty() || results[0].entry.isNull())
        throw DB::Exception(
                "PoolWithFailoverBase::getMany() returned less than min_entries entries.",
                DB::ErrorCodes::LOGICAL_ERROR);
    return results[0].entry;
}

template <typename TNestedPool>
std::vector<typename PoolWithFailoverBase<TNestedPool>::TryResult>
PoolWithFailoverBase<TNestedPool>::getMany(
        size_t min_entries, size_t max_entries, size_t max_tries,
        size_t max_ignored_errors,
        bool fallback_to_stale_replicas,
        const TryGetEntryFunc & try_get_entry,
        const GetPriorityFunc & get_priority)
{
    /// Update random numbers and error counts.
    PoolStates pool_states = updatePoolStates(max_ignored_errors);
    if (get_priority)
    {
        for (size_t i = 0; i < pool_states.size(); ++i)
            pool_states[i].priority = get_priority(i);
    }

    struct ShuffledPool
    {
        NestedPool * pool{};
        const PoolState * state{};
        size_t index = 0;
        size_t error_count = 0;
    };

    /// Sort the pools into order in which they will be tried (based on respective PoolStates).
    std::vector<ShuffledPool> shuffled_pools;
    shuffled_pools.reserve(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
        shuffled_pools.push_back(ShuffledPool{nested_pools[i].get(), &pool_states[i], i, 0});
    std::sort(
            shuffled_pools.begin(), shuffled_pools.end(),
            [](const ShuffledPool & lhs, const ShuffledPool & rhs)
            {
                return PoolState::compare(*lhs.state, *rhs.state);
            });

    /// We will try to get a connection from each pool until a connection is produced or max_tries is reached.
    std::vector<TryResult> try_results(shuffled_pools.size());
    size_t entries_count = 0;
    size_t usable_count = 0;
    size_t up_to_date_count = 0;
    size_t failed_pools_count = 0;

    /// At exit update shared error counts with error counts occurred during this call.
    SCOPE_EXIT(
    {
        std::lock_guard lock(pool_states_mutex);
        for (const ShuffledPool & pool: shuffled_pools)
        {
            auto & pool_state = shared_pool_states[pool.index];
            pool_state.error_count = std::min<UInt64>(max_error_cap, pool_state.error_count + pool.error_count);
        }
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
            result = try_get_entry(*shuffled_pool.pool, fail_message);

            if (!fail_message.empty())
                fail_messages += fail_message + '\n';

            if (!result.entry.isNull())
            {
                ++entries_count;
                if (result.is_usable)
                {
                    ++usable_count;
                    if (result.is_up_to_date)
                        ++up_to_date_count;
                }
            }
            else
            {
                LOG_WARNING(log, "Connection failed at try №{}, reason: {}", (shuffled_pool.error_count + 1), fail_message);
                ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

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
        throw DB::NetException(
                "All connection tries failed. Log: \n\n" + fail_messages + "\n",
                DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);

    try_results.erase(
            std::remove_if(
                    try_results.begin(), try_results.end(),
                    [](const TryResult & r) { return r.entry.isNull() || !r.is_usable; }),
            try_results.end());

    /// Sort so that preferred items are near the beginning.
    std::stable_sort(
            try_results.begin(), try_results.end(),
            [](const TryResult & left, const TryResult & right)
            {
                return std::forward_as_tuple(!left.is_up_to_date, left.staleness)
                    < std::forward_as_tuple(!right.is_up_to_date, right.staleness);
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
        /// There is enough up-to-date entries.
        try_results.resize(up_to_date_count);
    }
    else
        throw DB::Exception(
                "Could not find enough connections to up-to-date replicas. Got: " + std::to_string(up_to_date_count)
                + ", needed: " + std::to_string(min_entries),
                DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);

    return try_results;
}

template <typename TNestedPool>
struct PoolWithFailoverBase<TNestedPool>::PoolState
{
    UInt64 error_count = 0;
    /// Priority from the <remote_server> configuration.
    Int64 config_priority = 1;
    /// Priority from the GetPriorityFunc.
    Int64 priority = 0;
    UInt32 random = 0;

    void randomize()
    {
        random = rng();
    }

    static bool compare(const PoolState & lhs, const PoolState & rhs)
    {
        return std::forward_as_tuple(lhs.error_count, lhs.config_priority, lhs.priority, lhs.random)
             < std::forward_as_tuple(rhs.error_count, rhs.config_priority, rhs.priority, rhs.random);
    }

private:
    std::minstd_rand rng = std::minstd_rand(randomSeed());
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

        time_t current_time = time(nullptr);

        if (last_error_decrease_time)
        {
            time_t delta = current_time - last_error_decrease_time;

            if (delta >= 0)
            {
                const UInt64 MAX_BITS = sizeof(UInt64) * CHAR_BIT;
                size_t shift_amount = MAX_BITS;
                /// Divide error counts by 2 every decrease_error_period seconds.
                if (decrease_error_period)
                    shift_amount = delta / decrease_error_period;
                /// Update time but don't do it more often than once a period.
                /// Else if the function is called often enough, error count will never decrease.
                if (shift_amount)
                    last_error_decrease_time = current_time;

                if (shift_amount >= MAX_BITS)
                {
                    for (auto & state : shared_pool_states)
                        state.error_count = 0;
                }
                else if (shift_amount)
                {
                    for (auto & state : shared_pool_states)
                        state.error_count >>= shift_amount;
                }
            }
        }
        else
            last_error_decrease_time = current_time;

        result.assign(shared_pool_states.begin(), shared_pool_states.end());
    }

    /// distributed_replica_max_ignored_errors
    for (auto & state : result)
        state.error_count = std::max<UInt64>(0, state.error_count - max_ignored_errors);

    return result;
}
