#include <gtest/gtest.h>
#include <Common/PageCache.h>
#include <thread>
#include <chrono>

/// Test that PageCache::autoResize correctly tracks peak memory usage over the history window.
///
/// Bug: In PageCache.cpp, autoResize() computes `now` in microseconds but divides by
/// history_window.count() which returns milliseconds. This unit mismatch makes the bucket
/// value ~1000x too large, so on every call the condition `bucket > cur_bucket + 1` is true,
/// resetting peak_memory_buckets to {0, 0}. This means peak memory is never retained across
/// calls, defeating the purpose of the history window.

TEST(PageCacheAutoResize, PeakMemoryRetainedWithinHistoryWindow)
{
    constexpr size_t MB = 1024 * 1024;

    /// Create a PageCache with:
    ///   history_window = 2000ms (2 seconds)
    ///   cache_policy = "LRU"
    ///   size_ratio = 0.5
    ///   min_size = 0
    ///   max_size = 100 MB
    ///   free_memory_ratio = 0.0 (so reduced_limit == memory_limit)
    ///   num_shards = 1
    auto cache = DB::PageCache(
        std::chrono::milliseconds(2000),
        "LRU",
        0.5,
        /*min_size_in_bytes=*/ 0,
        /*max_size_in_bytes=*/ 100 * MB,
        /*free_memory_ratio=*/ 0.0,
        /*num_shards=*/ 1);

    constexpr size_t memory_limit = 100 * MB;

    /// Call 1: Report high memory usage (90 MB).
    /// peak should be 90 MB, target_size = 100 MB - 90 MB = 10 MB.
    cache.autoResize(static_cast<Int64>(90 * MB), memory_limit);
    size_t max_after_high = cache.maxSizeInBytes();

    /// Sleep 100ms - well within the 2000ms history window.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// Call 2: Report low memory usage (10 MB).
    /// Correct behavior: peak is still 90 MB (from within the history window),
    ///   so target_size = 100 MB - 90 MB = 10 MB, maxSizeInBytes stays ~10 MB.
    /// Buggy behavior: peak_memory_buckets were reset to {0, 0} due to the unit mismatch,
    ///   so peak = 10 MB, target_size = 100 MB - 10 MB = 90 MB, maxSizeInBytes jumps to ~90 MB.
    cache.autoResize(static_cast<Int64>(10 * MB), memory_limit);
    size_t max_after_low = cache.maxSizeInBytes();

    /// With correct behavior, cache max size should NOT dramatically increase when memory
    /// usage drops within the history window. The peak from the first call should be remembered.
    ///
    /// If the bug is present, max_after_low will be ~90 MB (9x larger than max_after_high).
    /// If the bug is fixed, max_after_low should be ~10 MB (same as max_after_high).
    ///
    /// We check that max_after_low is not more than 2x max_after_high, which gives generous
    /// margin while still catching the 9x jump caused by the bug.
    EXPECT_LE(max_after_low, max_after_high * 2)
        << "Peak memory was not retained within the history window. "
        << "max_after_high=" << max_after_high << " max_after_low=" << max_after_low
        << ". This indicates the unit mismatch bug in autoResize "
        << "(microseconds divided by milliseconds count).";
}

/// Additional test: verify that calling autoResize rapidly (no sleep) also retains peak.
TEST(PageCacheAutoResize, PeakMemoryRetainedNoSleep)
{
    constexpr size_t MB = 1024 * 1024;

    auto cache = DB::PageCache(
        std::chrono::milliseconds(1000),
        "LRU",
        0.5,
        /*min_size_in_bytes=*/ 0,
        /*max_size_in_bytes=*/ 100 * MB,
        /*free_memory_ratio=*/ 0.0,
        /*num_shards=*/ 1);

    constexpr size_t memory_limit = 100 * MB;

    /// Report high memory usage.
    cache.autoResize(static_cast<Int64>(80 * MB), memory_limit);
    size_t max_after_high = cache.maxSizeInBytes();

    /// Immediately report low memory usage (no sleep at all).
    /// Even with sub-millisecond time difference, the history window is 1000ms,
    /// so the peak should definitely be retained.
    cache.autoResize(static_cast<Int64>(5 * MB), memory_limit);
    size_t max_after_low = cache.maxSizeInBytes();

    EXPECT_LE(max_after_low, max_after_high * 2)
        << "Peak memory not retained even with immediate successive calls. "
        << "max_after_high=" << max_after_high << " max_after_low=" << max_after_low;
}
