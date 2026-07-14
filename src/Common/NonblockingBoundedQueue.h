#pragma once

#include <atomic>
#include <vector>
#include <Common/BitHelpers.h>

/// Vyukov queue.
/// https://web.archive.org/web/20170205113402/http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
/// Lock-free. Fixed preallocated capacity.
/// No blocking operations, only tryPush/tryPop; you may need a separate condition_variable/futex/etc or busy-wait.
/// Most efficient when used as SPSC and the queue stays nonempty.
/// If there are multiple producers, they have contention on the queue tail.
/// If there are multiple consumers, they have contention on the queue head.
/// If the queue has <=1 element, the producer and consumer have contention on that element
/// (but this contention shouldn't slow down the producer, if the consumer busy-waits).
///
/// Background:
/// (Optional reading. This is one possible way to look at task queues. May or may not be a good one
///  in any given situation.)
///
/// When using a task queue to pass requests from one thread to another, it makes sense for the task
/// queue to have these properties:
///  * Push shouldn't become more expensive as the queue becomes nearly empty. Otherwise there's a
///    bad metastable state: cpu-bound producer fell behind and stays behind because of the
///    increased queue push overhead.
///  * Symmetrically, pop shouldn't become more expensive as the queue becomes nearly full.
///    Otherwise there's a similar feedback where the consumer's cpu-boundedness gets worse as it
///    falls behind more.
///
/// A blocking queue fails at both:
/// When pushing to an empty queue the producer needs to do futex wake to unblock the waiting consumer.
/// When popping from a full queue, the consumer needs to do futex wake to unblock the waiting producer.
/// This becomes noticeable somewhere in hundreds of thousands to millions of requests per second.
///
/// So in high-throughput system it may make sense to avoid futexes and use busy wait in threads
/// that may be a cpu bottleneck.
/// This queue is suitable for such usage. Or it can be paired with a condition_variable or whatever.
///
/// On the other hand, pushing millions of items/s through a task queue is questionable.
/// Normally you should batch tasks. E.g. if your request latency is 1ms, you can probably afford to
/// inject another 0.5ms of latency to batch together 0.5ms worth of requests, bringing the rate
/// down to 2000 batches/s, which should be ok for any kind of queue implementation.
///
/// So, the main possible reasons (known to me) to use a fancy fast queue are:
///  * There are many producers, so batching seems impossible.
///    (But why are there many producers? Are requests arriving from many sockets, and we have a
///     thread/fiber per socket? Then use one thread to read from many sockets and do batching in that thread.
///     Just a few such threads should be enough to saturate network bandwidth.)
///  * There's some kind of many-to-many shuffle based on a key, so batching seems impossible.
///    (Probably still can batch separately for each <producer thread, consumer thread> pair.
///     It'd be n^2 times less effective than global batching, but probably still not 10^6 batches/s.)
///  * You don't want to worry about all of that. Instead of doing complicated thinking and profiling
///    to figure out what kinds of inefficiency you can get away with, you prefer to pick the most
///    efficient design from the start.
///    Sometimes this saves time (less profiling and rewriting/optimizing), sometimes it wastes time.
template <typename T>
class NonblockingBoundedQueue
{
public:
    static_assert(std::is_nothrow_move_assignable_v<T>, "NonblockingBoundedQueue requires noexcept move assignment to avoid permanent deadlock");
    static_assert(std::is_default_constructible_v<T>, "NonblockingBoundedQueue requires default constructor");

    NonblockingBoundedQueue() = default; // must call init() before using
    explicit NonblockingBoundedQueue(size_t min_capacity)
    {
        init(min_capacity);
    }

    void init(size_t min_capacity)
    {
        chassert(!mask);
        /// (Capacity must be at least 2 to make `pos + 1` different from `pos + capacity`.)
        min_capacity = std::max<size_t>(2, roundUpToPowerOfTwoOrZero(min_capacity));
        chassert(isPowerOf2(min_capacity));
        mask = min_capacity - 1;
        slots = std::vector<Slot>(min_capacity);
        for (size_t i = 0; i < slots.size(); ++i)
            slots[i].pos.store(i, std::memory_order_relaxed);
    }

    /// `value` is moved-out iff the return value is true.
    bool tryPush(T && value)
    {
        chassert(mask);
        size_t pos = enqueue_pos.load(std::memory_order_relaxed);
        while (true)
        {
            Slot & slot = slots[pos & mask];
            size_t slot_pos = slot.pos.load(std::memory_order_acquire);
            if (slot_pos < pos)
            {
                // This slot's previous value wasn't consumed. Queue is full.
                return false;
            }
            else if (slot_pos > pos)
            {
                // This slot already has a new value. Another producer won a race.
                pos = enqueue_pos.load(std::memory_order_relaxed);
                continue;
            }
            else if (!enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            {
                // Another producer won a different race.
                continue;
            }

            slot.value = std::move(value);
            slot.pos.store(pos + 1, std::memory_order_release);
            return true;
        }
    }

    bool tryPop(T & out_value)
    {
        chassert(mask);
        size_t pos = dequeue_pos.load(std::memory_order_relaxed);
        while (true)
        {
            Slot & slot = slots[pos & mask];
            size_t slot_pos = slot.pos.load(std::memory_order_acquire);
            if (slot_pos < pos + 1)
            {
                /// Queue is empty.
                return false;
            }
            else if (slot_pos > pos + 1)
            {
                /// Another consumer won a race.
                pos = dequeue_pos.load(std::memory_order_relaxed);
                continue;
            }
            else if (!dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            {
                /// Another consumer won a different race.
                continue;
            }

            out_value = std::move(slot.value);
            slot.pos.store(pos + mask + 1, std::memory_order_release);
            return true;
        }
    }

    /// Imprecise if called in parallel with other operations.
    size_t size() const
    {
        size_t y = dequeue_pos.load(std::memory_order_relaxed);
        size_t x = enqueue_pos.load(std::memory_order_relaxed);
        return x - std::min(x, y); // max(0, x - y)
    }

private:
    struct alignas(DB::CH_CACHE_LINE_SIZE) Slot
    {
        std::atomic<size_t> pos;
        T value;
    };

    size_t mask = 0; // capacity - 1
    std::vector<Slot> slots;
    alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<size_t> enqueue_pos {};
    alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<size_t> dequeue_pos {};
};
