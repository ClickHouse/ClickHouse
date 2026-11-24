#include <gtest/gtest.h>
#include <config.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/Prefetcher.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

using namespace DB;
using namespace DB::Parquet;

/// `registerRange` inserts a request and then grows the range vector. When the growth fails, the request must still
/// be cancelled, or the destructor of `Prefetcher` finds a request that is not cancelled. Every allocation of the
/// failing call is rejected, after a varying number of successful calls, so that some of the failing calls fail
/// on the growth of the range vector, after the request is inserted.
TEST(ParquetPrefetcher, RegisterRangeUnderMemoryLimit)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;

    /// `init` checks the magic bytes.
    std::string file(1 << 16, 'x');
    file.replace(0, 4, "PAR1");
    file.replace(file.size() - 4, 4, "PAR1");

    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_thread_hard_limit = thread_tracker.getHardLimit();
    const Int64 saved_total_hard_limit = total_memory_tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();

    auto lift_limits = [&]
    {
        total_memory_tracker.setHardLimit(saved_total_hard_limit);
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    };
    SCOPE_EXIT_SAFE(lift_limits());

    size_t num_failed = 0;
    for (size_t num_registered = 0; num_registered < 64; ++num_registered)
    {
        ReadBufferFromMemory in(file.data(), file.size());
        Prefetcher prefetcher;
        prefetcher.init(&in, ReadOptions{}, /*parser_shared_resources_=*/ nullptr);

        /// Destroyed before `prefetcher`.
        std::vector<PrefetchHandle> handles;
        handles.reserve(num_registered + 1);
        for (size_t i = 0; i < num_registered; ++i)
            handles.push_back(prefetcher.registerRange(i * 16, 16, /*likely_to_be_used=*/ false));

        /// Make even a tiny allocation reach the trackers and fail: no per-thread untracked buffer, limits of one byte.
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
        CurrentThread::get().untracked_memory_limit = 0;
        CurrentThread::flushUntrackedMemory();
        total_memory_tracker.resetCounters();
        thread_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1);
        thread_tracker.setHardLimit(1);

        int code = 0;
        try
        {
            handles.push_back(prefetcher.registerRange(num_registered * 16, 16, /*likely_to_be_used=*/ false));
        }
        catch (const Exception & e)
        {
            code = e.code();
        }

        /// Lift the limits before anything else, the assertions allocate.
        lift_limits();

        if (code)
        {
            EXPECT_EQ(code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
            ++num_failed;
        }
    }

    /// The range vector grows at least six times in 64 calls.
    EXPECT_GE(num_failed, 6u);
}

#endif
