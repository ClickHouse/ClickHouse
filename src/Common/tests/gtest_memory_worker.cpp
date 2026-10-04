#include <gtest/gtest.h>

#include "config.h"

#if USE_JEMALLOC

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <fmt/format.h>

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

/// Adjusts only `total_memory_tracker`. On the gtest main thread `CurrentMemoryTracker` would also charge the
/// thread's own tracker, which nothing corrects afterwards, so its skew would leak into later tests in the binary.
/// A thread without `ThreadStatus` accounts to the global tracker directly.
void adjustTotalMemoryTrackerOnly(Int64 delta)
{
    std::thread([delta]
    {
        if (delta >= 0)
            std::ignore = CurrentMemoryTracker::alloc(delta);
        else
            std::ignore = CurrentMemoryTracker::free(-delta);
    }).join();
}

/// Keeps freed pages dirty (resident) for the whole test: jemalloc's background threads purge them gradually
/// over `dirty_decay_ms` (5 s by default), which is enough to erode the resident-vs-allocated gap the test relies
/// on within its runtime on a busy machine. Set per arena, like `MemoryWorker::setDirtyDecayForAllArenas`:
/// `arenas.dirty_decay_ms` only affects arenas created afterwards.
struct DirtyDecayDisabler
{
    ssize_t previous_decay_ms = DB::Jemalloc::getValue<ssize_t>("arenas.dirty_decay_ms");

    DirtyDecayDisabler()
    {
        setForAllArenas(-1);
    }

    ~DirtyDecayDisabler()
    {
        setForAllArenas(previous_decay_ms);
    }

    static void setForAllArenas(ssize_t decay_ms)
    {
        const unsigned narenas = DB::Jemalloc::getValue<unsigned>("arenas.narenas");
        for (unsigned i = 0; i < narenas; ++i)
            DB::Jemalloc::setValue<ssize_t>(fmt::format("arena.{}.dirty_decay_ms", i).c_str(), decay_ms);
    }
};

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/117681.
///
/// When the global memory tracker goes negative (memory allocated before the tracker was initialized
/// is freed later), `MemoryWorker` re-baselines it once. The baseline must be the amount of live
/// allocations, not the resident memory: right after a lot of memory is freed, the resident memory
/// still includes the freed pages until jemalloc purges them, and nothing lowers the tracker afterwards,
/// so a tracker re-baselined to it stays pinned near the previous peak and rejects every allocation.
///
/// The body is shared by two tests, one per memory usage source. With the cgroup-backed source (the default
/// on Linux) the worker never refreshes the jemalloc statistics epoch for its own needs, so the correction
/// has to refresh it itself, otherwise it would read a `stats.allocated` snapshot that predates the frees.
void testNegativeTrackerIsCorrectedToAllocatedNotResident(DB::MemoryWorker::MemoryUsageSource expected_source)
{
    const DirtyDecayDisabler dirty_decay_disabler;

    /// Allocate and free a lot of memory, like a large query does. jemalloc keeps the freed pages
    /// dirty for a while (`dirty_decay_ms`), so the resident memory stays far above the live allocations.
    constexpr size_t chunk_size = MEBIBYTE;
    constexpr size_t chunks = 512;
    Int64 allocated_at_peak = 0;
    {
        std::vector<std::unique_ptr<char[]>> memory;
        memory.reserve(chunks);
        for (size_t i = 0; i < chunks; ++i)
        {
            memory.emplace_back(new char[chunk_size]);
            memset(memory.back().get(), 1, chunk_size);
        }

        /// The last refresh of the jemalloc statistics epoch before the worker runs happens here, while the
        /// memory is still live. The worker must refresh the epoch itself before reading `stats.allocated`,
        /// otherwise it would re-baseline the tracker to this stale peak snapshot.
        allocated_at_peak = readJemallocStats().allocated;
    }
    ASSERT_GT(allocated_at_peak, static_cast<Int64>(chunks * chunk_size));

    /// Drive the tracker negative in the same way as late frees of memory it never saw allocated.
    const Int64 amount_before = total_memory_tracker.get();
    adjustTotalMemoryTrackerOnly(-(std::max<Int64>(amount_before, 0) + 64 * MEBIBYTE));
    ASSERT_LT(total_memory_tracker.get(), 0);

    DB::MemoryWorkerConfig config;
    config.rss_update_period_ms = 10;
    config.use_cgroup = expected_source == DB::MemoryWorker::MemoryUsageSource::Cgroups;
    {
        DB::MemoryWorker worker(config, nullptr);
        if (worker.getSource() != expected_source)
        {
            /// Restore the tracker so that the skipped test does not leave it negative for the others.
            adjustTotalMemoryTrackerOnly(-total_memory_tracker.get());
            GTEST_SKIP() << "The requested memory usage source is not available in this environment";
        }
        worker.start();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (total_memory_tracker.get() < 0 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const Int64 corrected = total_memory_tracker.get();
    /// Read after the correction: the worker has re-baselined the tracker by now, and the freed pages are still
    /// resident (decay is disabled and the worker does not purge them; `purge_dirty_pages_threshold_ratio` is 0).
    const JemallocStats stats = readJemallocStats();
    const Int64 gap = stats.resident - stats.allocated;
    ASSERT_GT(gap, 256 * MEBIBYTE) << "resident: " << stats.resident << ", allocated: " << stats.allocated;
    ASSERT_GT(allocated_at_peak - stats.allocated, 256 * MEBIBYTE)
        << "allocated at peak: " << allocated_at_peak << ", allocated: " << stats.allocated;

    ASSERT_GE(corrected, 0);
    /// Correcting to the resident memory would land at least `gap` above the live allocations, and correcting to
    /// a stale `stats.allocated` snapshot would land at least `allocated_at_peak - stats.allocated` above them.
    EXPECT_LT(corrected, stats.allocated + 64 * MEBIBYTE)
        << "resident: " << stats.resident << ", allocated: " << stats.allocated << ", allocated at peak: " << allocated_at_peak;
}

}

TEST(MemoryWorker, NegativeTrackerIsCorrectedToAllocatedNotResident)
{
    testNegativeTrackerIsCorrectedToAllocatedNotResident(DB::MemoryWorker::MemoryUsageSource::Jemalloc);
}

/// The shipped configuration: `MemoryWorkerConfig::use_cgroup` defaults to `true`, and on Linux the worker reads
/// the resident memory from the cgroup files. Skipped where no cgroup memory controller is available.
TEST(MemoryWorker, NegativeTrackerIsCorrectedToAllocatedNotResidentWithCgroupSource)
{
    testNegativeTrackerIsCorrectedToAllocatedNotResident(DB::MemoryWorker::MemoryUsageSource::Cgroups);
}

#endif
