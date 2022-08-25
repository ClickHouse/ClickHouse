#pragma once

#include <Common/logger_useful.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

// This struct is used for the comparison of query memory usage.
struct OvercommitRatio
{
    OvercommitRatio(Int64 committed_, Int64 soft_limit_)
        : committed(committed_)
        , soft_limit(soft_limit_)
    {}

    friend bool operator<(OvercommitRatio const & lhs, OvercommitRatio const & rhs) noexcept
    {
        Int128 lhs_committed = lhs.committed, lhs_soft_limit = lhs.soft_limit;
        Int128 rhs_committed = rhs.committed, rhs_soft_limit = rhs.soft_limit;
        // (a / b < c / d) <=> (a * d < c * b)
        return (lhs_committed * rhs_soft_limit) < (rhs_committed * lhs_soft_limit)
            || (lhs_soft_limit == 0 && rhs_soft_limit > 0)
            || (lhs_committed == 0 && rhs_committed == 0 && lhs_soft_limit > rhs_soft_limit);
    }

    // actual query memory usage
    Int64 committed;
    // guaranteed amount of memory query can use
    Int64 soft_limit;
};

class MemoryTracker;

enum class OvercommitResult
{
    NONE,
    DISABLED,
    MEMORY_FREED,
    SELECTED,
    TIMEOUTED,
    NOT_ENOUGH_FREED,
};

enum class QueryCancellationState
{
    NONE     = 0,  // Hard limit is not reached, there is no selected query to kill.
    SELECTED = 1,  // Hard limit is reached, query to stop was chosen but it still is not aware of cancellation.
    RUNNING  = 2,  // Hard limit is reached, selected query has started the process of cancellation.
};

// Usually it's hard to set some reasonable hard memory limit
// (especially, the default value). This class introduces new
// mechanisim for the limiting of memory usage.
// Soft limit represents guaranteed amount of memory query/user
// may use. It's allowed to exceed this limit. But if hard limit
// is reached, query with the biggest overcommit ratio
// is killed to free memory.
struct OvercommitTracker : boost::noncopyable
{
    OvercommitResult needToStopQuery(MemoryTracker * tracker, Int64 amount);

    void tryContinueQueryExecutionAfterFree(Int64 amount);

    void onQueryStop(MemoryTracker * tracker);

    virtual ~OvercommitTracker() = default;

protected:
    explicit OvercommitTracker(std::mutex & global_mutex_);

    virtual void pickQueryToExcludeImpl() = 0;

    // This mutex is used to disallow concurrent access
    // to picked_tracker and cancelation_state variables.
    std::mutex overcommit_m;
    std::condition_variable cv;

    // Specifies memory tracker of the chosen to stop query.
    // If soft limit is not set, all the queries which reach hard limit must stop.
    // This case is represented as picked tracker pointer is set to nullptr and
    // overcommit tracker is in SELECTED state.
    MemoryTracker * picked_tracker;

private:

    void pickQueryToExclude()
    {
        if (cancellation_state == QueryCancellationState::NONE)
        {
            pickQueryToExcludeImpl();
            cancellation_state = QueryCancellationState::SELECTED;
        }
    }

    void reset() noexcept
    {
        picked_tracker = nullptr;
        cancellation_state = QueryCancellationState::NONE;
        freed_memory = 0;

        next_id = 0;
        id_to_release = 0;

        allow_release = true;
    }

    void releaseThreads();

    QueryCancellationState cancellation_state;

    // Global mutex which is used in ProcessList to synchronize
    // insertion and deletion of queries.
    // OvercommitTracker::pickQueryToExcludeImpl() implementations
    // require this mutex to be locked, because they read list (or sublist)
    // of queries.
    std::mutex & global_mutex;
    Int64 freed_memory;
    Int64 required_memory;

    size_t next_id; // Id provided to the next thread to come in OvercommitTracker
    size_t id_to_release; // We can release all threads with id smaller than this

    bool allow_release;
};

namespace DB
{
    class ProcessList;
    struct ProcessListForUser;
}

struct UserOvercommitTracker : OvercommitTracker
{
    explicit UserOvercommitTracker(DB::ProcessList * process_list, DB::ProcessListForUser * user_process_list_);

    ~UserOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override;

private:
    DB::ProcessListForUser * user_process_list;
};

struct GlobalOvercommitTracker : OvercommitTracker
{
    explicit GlobalOvercommitTracker(DB::ProcessList * process_list_);

    ~GlobalOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override;

private:
    DB::ProcessList * process_list;
};

// This class is used to disallow tracking during logging to avoid deadlocks.
struct OvercommitTrackerBlockerInThread
{
    OvercommitTrackerBlockerInThread() { ++counter; }
    ~OvercommitTrackerBlockerInThread() { --counter; }

    OvercommitTrackerBlockerInThread(OvercommitTrackerBlockerInThread const &) = delete;
    OvercommitTrackerBlockerInThread & operator=(OvercommitTrackerBlockerInThread const &) = delete;

    static bool isBlocked() { return counter > 0; }

private:
    static thread_local size_t counter;
};
