#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>


namespace DB
{

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
        ~Slot();

    private:
        friend struct Allocation; // for ctor

        explicit Slot(AllocationPtr && allocation_);

        AllocationPtr allocation;
    };

    // FIXME: have to be unique_ptr, but ThreadFromGlobalPool does not support move semantics yet
    using SlotPtr = std::shared_ptr<Slot>;

    // Manages group of slots for a single query, see ConcurrencyControl::allocate(min, max)
    struct Allocation : std::enable_shared_from_this<Allocation>, boost::noncopyable
    {
        ~Allocation();

        // Take one already granted slot if available. Lock-free iff there is no granted slot.
        [[nodiscard]] SlotPtr tryAcquire();

        SlotCount grantedCount() const;

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControl; // for grant(), free() and ctor

        Allocation(ConcurrencyControl & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_ = {});

        auto cancel()
        {
            std::unique_lock lock{mutex};
            return std::pair{allocated - released,
                allocated < limit ?
                    std::optional<Waiters::iterator>(waiter) :
                    std::optional<Waiters::iterator>()};
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant();

        // Release one slot and grant it to other allocation if required
        void release();

        ConcurrencyControl & parent;
        const SlotCount limit;

        std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount allocated; // allocated total (including already `released`)
        SlotCount released = 0;

        std::atomic<SlotCount> granted; // allocated, but not yet acquired

        const Waiters::iterator waiter; // iterator to itself in Waiters list; valid iff allocated < limit
    };

public:
    ConcurrencyControl();

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControl();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    [[nodiscard]] AllocationPtr allocate(SlotCount min, SlotCount max);

    void setMaxConcurrency(SlotCount value);

    static ConcurrencyControl & instance();

private:
    friend struct Allocation; // for free() and release()

    void free(Allocation * allocation);

    void release(SlotCount amount);

    // Round-robin scheduling of available slots among waiting allocations
    void schedule(std::unique_lock<std::mutex> &);

    SlotCount available(std::unique_lock<std::mutex> &) const;

    std::mutex mutex;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
    SlotCount max_concurrency = Unlimited;
    SlotCount cur_concurrency = 0;
};

}
