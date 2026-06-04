#pragma once

#include <Common/Stopwatch.h>
#include <atomic>

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
            UInt64 time_value = cur_num_threads == 0 ? (clock_gettime_ns() - query_start_time) << 16 : 
                                                        old_value & ~MASK_16_BIT;
            new_value = time_value | (cur_num_threads + 1);
        } while (!threads_and_time.compare_exchange_weak(old_value, new_value, std::memory_order_release, std::memory_order_relaxed));
    }

    void onLeave()
    {
        UInt64 new_value = 0;
        UInt64 old_value = threads_and_time.load(std::memory_order_acquire);
        bool last_thread = false;
        do 
        {
            new_value = old_value - 1;
            /// Extract the number of threads after decrement
            last_thread = (new_value & MASK_16_BIT) == 0;
        } while (!threads_and_time.compare_exchange_weak(old_value, new_value, std::memory_order_release, std::memory_order_relaxed));

        if (last_thread)
        {
            /// new_value contains the old entry time, so we substract it
            UInt64 start_time = (new_value >> 16);
            wall_clock_time.fetch_add((clock_gettime_ns() - query_start_time) - start_time, std::memory_order_release);
        }
    }

    UInt64 getStepWallTime() { return wall_clock_time.load(std::memory_order_acquire); }

private:

    /// The layout: [ 48-bit time ][ 16-bit thread counter ] 
    std::atomic<UInt64> threads_and_time = 0;

    std::atomic<UInt64> wall_clock_time = 0;
    const UInt64 query_start_time = 0;
    constexpr static UInt64 MASK_16_BIT = 0xFFFF;
};
