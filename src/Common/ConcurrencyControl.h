#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <mutex>
#include <memory>
#include <list>
#include <condition_variable>

/*
 * Controls how many threads can be allocated for a query (or another activity).
 * There is a limited amount of slots for threads. It can be set with `setMaxConcurrency(limit)`.
 *
 * Lifecycle of a slot: free -> granted -> acquired -> free.
 * free: slot is available to be allocated by any query.
 * granted: slot is allocated by specific query, but not yet acquired by any thread.
 * acquired: slot is allocated by specific query and acquired by a thread.
 *
 * USAGE:
 *   1. Create an allocation for a query:
 *      `auto slots = ConcurrencyControl::instance().allocate(min, max);`
 *      It will allocate at least `min` and at most `max` slots.
 *      Note that `min` slots are granted immediately, but other `max - min` may be granted later.
 *   2. For every thread a slot has to be acquired from that allocation:
 *      `while (auto slot = slots->tryAcquire()) createYourThread([slot = std::move(slot)] { ... });`
 *      This snippet can be used at query startup and for upscaling later.
 * (both functions are non-blocking)
 *
 * Released slots are distributed between waiting allocations in a round-robin manner to provide fairness.
 * Oversubscription is possible: total amount of allocated slots can exceed `setMaxConcurrency(limit)`
 * because `min` amount of slots is allocated for each query unconditionally.
 */
class ConcurrencyControl : boost::noncopyable
{
public:
    struct Allocation;
    using AllocationPtr = std::shared_ptr<Allocation>;
    using Slots = UInt64;
    using Waiters = std::list<Allocation *>;

    // Scoped guard for acquired slot, see Allocation::tryAcquire()
    struct Slot : boost::noncopyable
    {
        ~Slot()
        {
            allocation->release();
        }

    private:
        friend struct Allocation; // for ctor

        explicit Slot(AllocationPtr && allocation_)
            : allocation(std::move(allocation_))
        {}

        AllocationPtr allocation;
    };

    // FIXME: have to be unique_ptr, but ThreadFromGlobalPool does not support move semantics yet
    using SlotPtr = std::shared_ptr<Slot>;

    // Manages group of slots for a single query, see ConcurrencyControl::allocate(min, max)
    struct Allocation : std::enable_shared_from_this<Allocation>, boost::noncopyable
    {
        ~Allocation()
        {
            if (released == limit) // also equal to `allocated`: everything is already released
                return;

            std::unique_lock lock{parent.mutex};
            parent.cur_concurrency -= allocated - released;

            // Cancel waiting
            if (allocated < limit && waiter != parent.waiters.end())
                parent.waiters.erase(waiter);

            parent.schedule(lock);
        }

        // Take one already granted slot if available
        [[nodiscard]] SlotPtr tryAcquire()
        {
            std::unique_lock lock{mutex};
            if (!granted)
                return {};
            granted--;
            return SlotPtr(new Slot(shared_from_this()));
        }

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControl; // for grant() and ctor

        Allocation(ConcurrencyControl & concurrency_control, Slots min, Slots max)
            : parent(concurrency_control)
            , limit(std::max(max, min))
        {
            std::unique_lock lock{parent.mutex};

            // Acquire as much slots as we can, but not lower than `min`
            granted = allocated = std::max(min, std::min(limit, parent.available(lock)));
            parent.cur_concurrency += allocated;

            // Start waiting if more slots are required
            if (allocated < limit)
                waiter = parent.waiters.insert(parent.cur_waiter, this);
            else
                waiter = parent.waiters.end();
        }

        // Release one slot and grant it to other allocation if required
        void release()
        {
            std::unique_lock lock{parent.mutex};
            parent.cur_concurrency--;
            parent.schedule(lock);

            std::unique_lock lock2{mutex};
            released++;
            assert(released <= allocated);
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant()
        {
            std::unique_lock lock{mutex};
            granted++;
            allocated++;
            return allocated < limit;
            // WARNING: `waiter` iterator is invalidated after returning false
        }

        ConcurrencyControl & parent;
        Waiters::iterator waiter; // iterator to itself in Waiters list

        const Slots limit;

        std::mutex mutex; // the following values must be accessed under this mutex
        Slots allocated = 0; // allocated total (including already released)
        Slots granted = 0; // allocated, but not yet acquired
        Slots released = 0;
    };

public:
    ConcurrencyControl()
        : cur_waiter(waiters.end())
    {}

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControl()
    {
        assert(waiters.empty());
    }

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use Allocation::tryAcquire() to acquire allocated slot, before running a thread.
    [[nodiscard]] AllocationPtr allocate(Slots min, Slots max)
    {
        return AllocationPtr(new Allocation(*this, min, max));
    }

    void setMaxConcurrency(Slots value)
    {
        std::unique_lock lock{mutex};
        max_concurrency = std::max<Slots>(1, value); // never allow max_concurrency to be zero
        schedule(lock);
    }

    static ConcurrencyControl & instance()
    {
        static ConcurrencyControl result;
        return result;
    }

private:
    Slots available(std::unique_lock<std::mutex> &)
    {
        if (cur_concurrency < max_concurrency)
            return max_concurrency - cur_concurrency;
        else
            return 0;
    }

    // Round-robin scheduling of available slots among waiting allocations
    void schedule(std::unique_lock<std::mutex> &)
    {
        while (cur_concurrency < max_concurrency && !waiters.empty())
        {
            cur_concurrency++;
            if (cur_waiter == waiters.end())
                cur_waiter = waiters.begin();
            Allocation * allocation = *cur_waiter;
            if (allocation->grant())
                ++cur_waiter;
            else
                waiters.erase(cur_waiter++); // last required slot has just been granted -- stop waiting
        }
    }

    std::mutex mutex;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
    Slots max_concurrency = Slots(-1);
    Slots cur_concurrency = 0;
};
