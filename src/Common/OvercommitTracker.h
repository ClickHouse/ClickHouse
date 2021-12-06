#pragma once

#include <base/logger_useful.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

struct OvercommitRatio
{
    OvercommitRatio(Int64 committed_, Int64 soft_limit_)
        : committed(committed_)
        , soft_limit(soft_limit_)
    {}

    friend bool operator<(OvercommitRatio const& lhs, OvercommitRatio const& rhs) noexcept
    {
        // (a / b < c / d) <=> (a * d < c * b)
        return (lhs.committed * rhs.soft_limit) < (rhs.committed * lhs.soft_limit)
            || (lhs.soft_limit == 0 && rhs.soft_limit > 0)
            || (lhs.committed == 0 && rhs.committed == 0 && lhs.soft_limit > rhs.soft_limit);
    }

    Int64 committed;
    Int64 soft_limit;
};

class MemoryTracker;

struct OvercommitTracker : boost::noncopyable
{
    OvercommitTracker();

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

    friend struct BlockQueryIfMemoryLimit;
};

namespace DB
{
    class ProcessList;
    struct ProcessListForUser;
}

struct UserOvercommitTracker : OvercommitTracker
{
    explicit UserOvercommitTracker(DB::ProcessListForUser * user_process_list_);

    ~UserOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override final;

private:
    DB::ProcessListForUser * user_process_list;
    Poco::Logger * logger = &Poco::Logger::get("UserOvercommitTracker");
};

struct GlobalOvercommitTracker : OvercommitTracker
{
    explicit GlobalOvercommitTracker(DB::ProcessList * process_list_)
        : process_list(process_list_)
    {}

    ~GlobalOvercommitTracker() override = default;

protected:
    void pickQueryToExcludeImpl() override final;

private:
    DB::ProcessList * process_list;
    Poco::Logger * logger = &Poco::Logger::get("GlobalOvercommitTracker");
};

struct BlockQueryIfMemoryLimit
{
    BlockQueryIfMemoryLimit(OvercommitTracker const & overcommit_tracker)
        : mutex(overcommit_tracker.overcommit_m)
        , lk(mutex)
    {
        if (overcommit_tracker.cancelation_state == OvercommitTracker::QueryCancelationState::RUNNING)
        {
            overcommit_tracker.cv.wait_for(lk, overcommit_tracker.max_wait_time, [&overcommit_tracker]()
            {
                return overcommit_tracker.cancelation_state == OvercommitTracker::QueryCancelationState::NONE;
            });
        }
    }

    ~BlockQueryIfMemoryLimit() = default;

private:
    std::mutex & mutex;
    std::unique_lock<std::mutex> lk;
};
