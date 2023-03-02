#pragma once

#include <base/logger_useful.h>
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
        // (a / b < c / d) <=> (a * d < c * b)
        return (lhs.committed * rhs.soft_limit) < (rhs.committed * lhs.soft_limit)
            || (lhs.soft_limit == 0 && rhs.soft_limit > 0)
            || (lhs.committed == 0 && rhs.committed == 0 && lhs.soft_limit > rhs.soft_limit);
    }

    // actual query memory usage
    Int64 committed;
    // guaranteed amount of memory query can use
    Int64 soft_limit;
};

class MemoryTracker;

// Usually it's hard to set some reasonable hard memory limit
// (especially, the default value). This class introduces new
// mechanisim for the limiting of memory usage.
// Soft limit represents guaranteed amount of memory query/user
// may use. It's allowed to exceed this limit. But if hard limit
// is reached, query with the biggest overcommit ratio
// is killed to free memory.
struct OvercommitTracker : boost::noncopyable
{
    explicit OvercommitTracker(std::mutex & global_mutex_);

    void setMaxWaitTime(UInt64 wait_time);

    bool needToStopQuery(MemoryTracker * tracker);

    void unsubscribe(MemoryTracker * tracker);

    virtual ~OvercommitTracker() = default;

protected:
    virtual void pickQueryToExcludeImpl() = 0;

    mutable std::mutex overcommit_m;
    mutable std::condition_variable cv;

    std::chrono::microseconds max_wait_time;

    enum class QueryCancelationState
    {
        NONE,
        RUNNING,
    };

    // Specifies memory tracker of the chosen to stop query.
    // If soft limit is not set, all the queries which reach hard limit must stop.
    // This case is represented as picked tracker pointer is set to nullptr and
    // overcommit tracker is in RUNNING state.
    MemoryTracker * picked_tracker;
    QueryCancelationState cancelation_state;

private:

    void pickQueryToExclude()
    {
        if (cancelation_state != QueryCancelationState::RUNNING)
        {
            pickQueryToExcludeImpl();
            cancelation_state = QueryCancelationState::RUNNING;
        }
    }

    std::mutex & global_mutex;
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
    void pickQueryToExcludeImpl() override final;

private:
    DB::ProcessListForUser * user_process_list;
};

struct GlobalOvercommitTracker : OvercommitTracker
{
    explicit GlobalOvercommitTracker(DB::ProcessList * process_list_);

    ~GlobalOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override final;

private:
    DB::ProcessList * process_list;
};
