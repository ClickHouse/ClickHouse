#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/thread_local_rng.h>
#include <Storages/MergeTree/MarkRange.h>

#include <random>

using namespace DB;

TEST(MarkRanges, MemoryTracking)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    total_memory_tracker.setHardLimit(1_KiB);
    CurrentThread::get().memory_tracker.setHardLimit(1_KiB);

    SCOPE_EXIT_SAFE(total_memory_tracker.setHardLimit(0));
    SCOPE_EXIT_SAFE(CurrentThread::get().memory_tracker.setHardLimit(0));

    constexpr size_t num_ranges = 1'000'000;
    std::uniform_int_distribution<size_t> dist(0, 1'000'000);

    MarkRanges ranges;

    try
    {
        for (size_t i = 0; i < num_ranges; ++i)
        {
            size_t begin = dist(thread_local_rng);
            size_t end = begin + dist(thread_local_rng) % 1000 + 1;
            ranges.emplace_back(begin, end);
        }
    }
    catch (DB::Exception &)
    {
        return;
    }

    FAIL() << "Expected memory limit exception was not thrown";
}
