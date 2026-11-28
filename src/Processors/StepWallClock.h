#pragma once

#include <Common/Stopwatch.h>
#include <atomic>

namespace DB
{
class StepWallClock
{
public:
    explicit StepWallClock(const UInt64 query_start)
    : query_start_time(query_start)
    {}

    void onEnter()
    {
        UInt64 new_value = 0;
        UInt64 old_value = threads_and_time.load(std::memory_order_acquire);
        do
        {
            UInt64 cur_num_threads = old_value & MASK_16_BIT;
            /// The 16-bit thread counter must not overflow into the time bits.
            chassert(cur_num_threads < MASK_16_BIT);

            UInt64 time_value = 0;
            if (cur_num_threads == 0)
            {
                const UInt64 elapsed = clock_gettime_ns() - query_start_time;
                /// The elapsed time is stored in the upper 48 bits, so it must fit into 48 bits.
                chassert(elapsed <= MAX_TIME_NS);
                time_value = elapsed << TIME_SHIFT;
            }
            else
                time_value = old_value & ~MASK_16_BIT;

            new_value = time_value | (cur_num_threads + 1);
        } while (!threads_and_time.compare_exchange_weak(old_value, new_value, std::memory_order_release, std::memory_order_relaxed));
    }

    void onLeave()
    {
        UInt64 new_value = 0;
        UInt64 exit_time = 0;
        UInt64 old_value = threads_and_time.load(std::memory_order_acquire);
        bool last_thread = false;
        do
        {
            /// onEnter and onLeave must be balanced: the counter must be positive here,
            /// otherwise the decrement borrows from the time bits.
            chassert((old_value & MASK_16_BIT) != 0);
            new_value = old_value - 1;
            exit_time = clock_gettime_ns() - query_start_time;
            /// Extract the number of threads after decrement
            last_thread = (new_value & MASK_16_BIT) == 0;
        } while (!threads_and_time.compare_exchange_weak(old_value, new_value, std::memory_order_release, std::memory_order_relaxed));

        if (last_thread)
        {
            /// new_value contains the old entry time, so we substract it
            UInt64 start_time = (new_value >> TIME_SHIFT);
            wall_clock_time.fetch_add(exit_time - start_time, std::memory_order_release);
        }
    }

    UInt64 getStepWallTime() const { return wall_clock_time.load(std::memory_order_acquire); }

private:

    /// The layout: [ 48-bit time ][ 16-bit thread counter ]
    std::atomic<UInt64> threads_and_time = 0;

    std::atomic<UInt64> wall_clock_time = 0;
    const UInt64 query_start_time = 0;
    /// The time occupies the upper 48 bits, the thread counter the lower 16.
    constexpr static UInt64 TIME_SHIFT = 16;
    constexpr static UInt64 MASK_16_BIT = 0xFFFF;
    constexpr static UInt64 MAX_TIME_NS = (static_cast<UInt64>(1) << (64ul - TIME_SHIFT)) - 1ul;
};
}
