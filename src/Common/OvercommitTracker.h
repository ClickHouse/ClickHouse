#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

struct OvercommitRatio
{
    OvercommitRatio(Int64 commited_, Int64 soft_limit_)
        : commited(commited_)
        , soft_limit(soft_limit_)
    {}

    friend bool operator<(OvercommitRatio const& lhs, OvercommitRatio const& rhs) noexcept
    {
        return (lhs.commited / lhs.soft_limit) < (rhs.commited / rhs.soft_limit);
    }

    Int64 commited;
    Int64 soft_limit;
};

class MemoryTracker;

struct OvercommitTracker : boost::noncopyable
{
    OvercommitTracker() = default;

    bool needToStopQuery(MemoryTracker * tracker);

    void unsubscribe(MemoryTracker * tracker)
    {
        std::unique_lock<std::mutex> lk(overcommit_m);
        if (tracker == picked_tracker)
        {            
            picked_tracker = nullptr;
            cancelation_state = QueryCancelationState::NONE;
            cv.notify_all();
        }
    }

    virtual ~OvercommitTracker() = default;

protected:
    virtual void pickQueryToExcludeImpl() = 0;

    mutable std::mutex overcommit_m;
    mutable std::condition_variable cv;

    enum class QueryCancelationState
    {
        NONE,
        RUNNING,
    };

    MemoryTracker * picked_tracker;
    QueryCancelationState cancelation_state = QueryCancelationState::NONE;

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
};

struct BlockQueryIfMemoryLimit
{
    BlockQueryIfMemoryLimit(OvercommitTracker const & overcommit_tracker)
        : mutex(overcommit_tracker.overcommit_m)
        , lk(mutex)
    {
        if (overcommit_tracker.cancelation_state == OvercommitTracker::QueryCancelationState::RUNNING)
        {
            //TODO: Add timeout
            overcommit_tracker.cv.wait(lk, [&overcommit_tracker]()
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
