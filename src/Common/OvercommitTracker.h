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
    void setMaxWaitTime(UInt64 wait_time);

    bool needToStopQuery(MemoryTracker * tracker, Int64 amount);

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

    std::chrono::microseconds max_wait_time;

    // Specifies memory tracker of the chosen to stop query.
    // If soft limit is not set, all the queries which reach hard limit must stop.
    // This case is represented as picked tracker pointer is set to nullptr and
    // overcommit tracker is in SELECTED state.
    MemoryTracker * picked_tracker;

    virtual Poco::Logger * getLogger() = 0;

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
        allow_release = true;
    }

    void releaseThreads();

    QueryCancellationState cancellation_state;

    std::unordered_map<MemoryTracker *, Int64> required_per_thread;

    // Global mutex which is used in ProcessList to synchronize
    // insertion and deletion of queries.
    // OvercommitTracker::pickQueryToExcludeImpl() implementations
    // require this mutex to be locked, because they read list (or sublist)
    // of queries.
    std::mutex & global_mutex;
    Int64 freed_memory;
    Int64 required_memory;

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

    Poco::Logger * getLogger() override final { return logger; }
private:
    DB::ProcessListForUser * user_process_list;
    Poco::Logger * logger = &Poco::Logger::get("UserOvercommitTracker");
};

struct GlobalOvercommitTracker : OvercommitTracker
{
    explicit GlobalOvercommitTracker(DB::ProcessList * process_list_);

    ~GlobalOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override;

    Poco::Logger * getLogger() override final { return logger; }
private:
    DB::ProcessList * process_list;
    Poco::Logger * logger = &Poco::Logger::get("GlobalOvercommitTracker");
};
