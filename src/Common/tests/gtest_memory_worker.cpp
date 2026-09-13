#include <gtest/gtest.h>

#include "config.h"

#if USE_JEMALLOC

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <Common/CurrentMemoryTracker.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryWorker.h>

namespace
{

constexpr Int64 MEBIBYTE = 1024 * 1024;

struct JemallocStats
{
    Int64 allocated;
    Int64 resident;
};

JemallocStats readJemallocStats()
{
    DB::Jemalloc::MibCache<uint64_t> epoch_mib{"epoch"};
    DB::Jemalloc::MibCache<size_t> allocated_mib{"stats.allocated"};
    DB::Jemalloc::MibCache<size_t> resident_mib{"stats.resident"};
    epoch_mib.setValue(0);
    return {static_cast<Int64>(allocated_mib.getValue()), static_cast<Int64>(resident_mib.getValue())};
}

}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/117681.
///
/// When the global memory tracker goes negative (memory allocated before the tracker was initialized
/// is freed later), `MemoryWorker` re-baselines it once. The baseline must be the amount of live
/// allocations, not the resident memory: right after a lot of memory is freed, the resident memory
/// still includes the freed pages until jemalloc purges them, and nothing lowers the tracker afterwards,
/// so a tracker re-baselined to it stays pinned near the previous peak and rejects every allocation.
TEST(MemoryWorker, NegativeTrackerIsCorrectedToAllocatedNotResident)
{
    /// Allocate and free a lot of memory, like a large query does. jemalloc keeps the freed pages
    /// dirty for a while (`dirty_decay_ms`), so the resident memory stays far above the live allocations.
    constexpr size_t chunk_size = MEBIBYTE;
    constexpr size_t chunks = 512;
    {
        std::vector<std::unique_ptr<char[]>> memory;
        memory.reserve(chunks);
        for (size_t i = 0; i < chunks; ++i)
        {
            memory.emplace_back(new char[chunk_size]);
            memset(memory.back().get(), 1, chunk_size);
        }
    }

    const JemallocStats stats = readJemallocStats();
    const Int64 gap = stats.resident - stats.allocated;
    ASSERT_GT(gap, 256 * MEBIBYTE) << "resident: " << stats.resident << ", allocated: " << stats.allocated;

    /// Drive the tracker negative in the same way as late frees of memory it never saw allocated.
    const Int64 amount_before = total_memory_tracker.get();
    std::ignore = CurrentMemoryTracker::free(std::max<Int64>(amount_before, 0) + 64 * MEBIBYTE);
    ASSERT_LT(total_memory_tracker.get(), 0);

    DB::MemoryWorkerConfig config;
    config.rss_update_period_ms = 10;
    config.use_cgroup = false;
    {
        DB::MemoryWorker worker(config, nullptr);
        worker.start();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (total_memory_tracker.get() < 0 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const Int64 corrected = total_memory_tracker.get();
    ASSERT_GE(corrected, 0);
    /// Correcting to the resident memory would land at least `gap` above the live allocations.
    EXPECT_LT(corrected, stats.allocated + 64 * MEBIBYTE) << "resident: " << stats.resident << ", allocated: " << stats.allocated;
}

#endif
