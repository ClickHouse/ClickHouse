#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <mutex>
#include <memory>
#include <list>
#include <condition_variable>

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

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
    using SlotCount = UInt64;
    using Waiters = std::list<Allocation *>;

    static constexpr SlotCount Unlimited = std::numeric_limits<SlotCount>::max();

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
            // We have to lock parent's mutex to avoid race with grant()
            // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
            parent.free(this);
        }

        // Take one already granted slot if available. Lock-free iff there is no granted slot.
        [[nodiscard]] SlotPtr tryAcquire()
        {
            SlotCount value = granted.load();
            while (value)
            {
                if (granted.compare_exchange_strong(value, value - 1))
                {
                    std::unique_lock lock{mutex};
                    return SlotPtr(new Slot(shared_from_this())); // can't use std::make_shared due to private ctor
                }
            }
            return {}; // avoid unnecessary locking
        }

        SlotCount grantedCount() const
        {
            return granted;
        }

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControl; // for grant(), free() and ctor

        Allocation(ConcurrencyControl & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_ = {})
            : parent(parent_)
            , limit(limit_)
            , allocated(granted_)
            , granted(granted_)
            , waiter(waiter_)
        {
            if (allocated < limit)
                *waiter = this;
        }

        auto cancel()
        {
            std::unique_lock lock{mutex};
            return std::pair{allocated - released,
                allocated < limit ?
                    std::optional<Waiters::iterator>(waiter) :
                    std::optional<Waiters::iterator>()};
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant()
        {
            std::unique_lock lock{mutex};
            granted++;
            allocated++;
            return allocated < limit;
        }

        // Release one slot and grant it to other allocation if required
        void release()
        {
            parent.release(1);
            std::unique_lock lock{mutex};
            released++;
            if (released > allocated)
                abort();
        }

        ConcurrencyControl & parent;
        const SlotCount limit;

        std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount allocated; // allocated total (including already `released`)
        SlotCount released = 0;

        std::atomic<SlotCount> granted; // allocated, but not yet acquired

        const Waiters::iterator waiter; // iterator to itself in Waiters list; valid iff allocated < limit
    };

public:
    ConcurrencyControl()
        : cur_waiter(waiters.end())
    {}

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControl()
    {
        if (!waiters.empty())
            abort();
    }

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    [[nodiscard]] AllocationPtr allocate(SlotCount min, SlotCount max)
    {
        if (min > max)
            throw DB::Exception("ConcurrencyControl: invalid allocation requirements", DB::ErrorCodes::LOGICAL_ERROR);

        std::unique_lock lock{mutex};

        // Acquire as much slots as we can, but not lower than `min`
        SlotCount granted = std::max(min, std::min(max, available(lock)));
        cur_concurrency += granted;

        // Create allocation and start waiting if more slots are required
        if (granted < max)
            return AllocationPtr(new Allocation(*this, max, granted,
                waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
        else
            return AllocationPtr(new Allocation(*this, max, granted));
    }

    void setMaxConcurrency(SlotCount value)
    {
        std::unique_lock lock{mutex};
        max_concurrency = std::max<SlotCount>(1, value); // never allow max_concurrency to be zero
        schedule(lock);
    }

    static ConcurrencyControl & instance()
    {
        static ConcurrencyControl result;
        return result;
    }

private:
    friend struct Allocation; // for free() and release()

    void free(Allocation * allocation)
    {
        // Allocation is allowed to be canceled even if there are:
        //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
        //  - `waiter`: active waiting for more slots to be allocated
        // Thus Allocation destruction may require the following lock, to avoid race conditions
        std::unique_lock lock{mutex};
        auto [amount, waiter] = allocation->cancel();

        cur_concurrency -= amount;
        if (waiter)
        {
            if (cur_waiter == *waiter)
                cur_waiter = waiters.erase(*waiter);
            else
                waiters.erase(*waiter);
        }
        schedule(lock);
    }

    void release(SlotCount amount)
    {
        std::unique_lock lock{mutex};
        cur_concurrency -= amount;
        schedule(lock);
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
                cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
        }
    }

    SlotCount available(std::unique_lock<std::mutex> &)
    {
        if (cur_concurrency < max_concurrency)
            return max_concurrency - cur_concurrency;
        else
            return 0;
    }

    std::mutex mutex;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
    SlotCount max_concurrency = Unlimited;
    SlotCount cur_concurrency = 0;
};
