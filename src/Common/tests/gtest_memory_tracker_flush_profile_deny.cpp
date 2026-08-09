#include "config.h"

#if USE_JEMALLOC

#include <Common/MemoryTracker.h>

/// DENY_ALLOCATIONS_IN_SCOPE only enforces (and the source guard under test is only
/// compiled) when MEMORY_TRACKER_DEBUG_CHECKS is defined, i.e. assertion-enabled builds
/// (debug, sanitizers). On release-based builds such as coverage, DENY is a no-op and
/// there is no allocation-denial to guard, so the flush path legitimately runs and the
/// invariant below does not hold there. Gate the test on the same macro the guard uses.
#ifdef MEMORY_TRACKER_DEBUG_CHECKS

#include <gtest/gtest.h>

#include <filesystem>
#include <system_error>

#include <unistd.h>

#include <base/scope_guard.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTrackerBlockerInThread.h>

using namespace DB;

namespace
{

/// flushProfile() asks jemalloc to write a heap dump into the current directory
/// (named "<prof_prefix>.<pid>.<seq>.heap"). Counting these files lets us assert
/// whether a flush actually happened, independent of how the dump path is formatted.
size_t countHeapDumps(const std::filesystem::path & dir)
{
    size_t count = 0;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
        if (it->path().extension() == ".heap")
            ++count;
    return count;
}

void removeHeapDumps(const std::filesystem::path & dir)
{
    std::error_code ec;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
        if (it->path().extension() == ".heap")
            std::filesystem::remove(it->path(), ec);
}

/// Trigger the global memory-limit-exceeded path that conditionally flushes the
/// jemalloc profile. adjustWithUntrackedMemory routes to the member
/// MemoryTracker::allocImpl with enforce_memory_limit=false (so no
/// MEMORY_LIMIT_EXCEEDED is thrown), and 4 MiB exceeds the 1-byte hard limit.
void hitMemoryExceededFlushPath()
{
    MemoryTracker tracker(/*parent_=*/nullptr, VariableContext::Global);
    tracker.setHardLimit(1);
    tracker.setJemallocFlushProfileOnMemoryExceeded(true);
    tracker.adjustWithUntrackedMemory(4 * 1024 * 1024);
}

}

/// The memory-limit-exceeded jemalloc flush path must be skipped while allocations
/// are denied: flushProfile() allocates (it formats the dump path with fmt::format
/// once the path outgrows the small-string buffer) and asks jemalloc to write a
/// heap dump. The path is reachable under DENY_ALLOCATIONS_IN_SCOPE (signal
/// handler, MergeTreeBackgroundExecutor flushing untracked memory) via the member
/// MemoryTracker::allocImpl, which bypasses CurrentMemoryTracker's own deny check.
/// updatePeak() already guards this; we pin the symmetric memory-exceeded path.
///
/// The invariant is asserted independently of PID length: while allocations are
/// denied, NO heap dump file may be produced. A fixed build skips flushProfile
/// entirely (0 files); an unfixed build either writes a dump (short PID -> the
/// assertion below fails) or allocates and aborts with LOGICAL_ERROR
/// "Memory tracker: allocations not allowed." (long PID -> the test process
/// crashes). Either way an unfixed build fails the test.
TEST(MemoryTrackerJemallocFlushProfile, MemoryExceededFlushSkippedUnderDeny)
{
    bool prof_active = false;
    if (!(Jemalloc::tryGetValue("prof.active", prof_active) && prof_active))
        GTEST_SKIP() << "jemalloc profiler not active in this build/runtime";

    /// Profile dumps land in the current directory; run from a scratch dir and
    /// remove it afterwards so the test is parallel-safe and leaves nothing.
    const auto old_cwd = std::filesystem::current_path();
    const auto scratch = std::filesystem::temp_directory_path()
        / ("ch_flush_profile_deny_" + std::to_string(getpid()));
    std::filesystem::create_directories(scratch);
    std::filesystem::current_path(scratch);
    SCOPE_EXIT({
        std::filesystem::current_path(old_cwd);
        std::error_code ec;
        std::filesystem::remove_all(scratch, ec);
    });

    /// Positive control: with allocations allowed, the very same code path must
    /// produce a heap dump here. This proves the dump mechanism is live in this
    /// runtime, so the "no dump under deny" assertion below cannot pass vacuously.
    hitMemoryExceededFlushPath();
    if (countHeapDumps(scratch) == 0)
        GTEST_SKIP() << "memory-exceeded jemalloc flush produced no dump in this runtime; cannot verify the guard";
    removeHeapDumps(scratch);
    ASSERT_EQ(countHeapDumps(scratch), 0u);

    /// The invariant: under DENY_ALLOCATIONS_IN_SCOPE the flush must be skipped,
    /// so no new heap dump appears. (The guard is checked before the per-callsite
    /// flush counter, so a fixed build never even reaches the flush throttle.)
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        hitMemoryExceededFlushPath();
    }

    EXPECT_EQ(countHeapDumps(scratch), 0u)
        << "jemalloc profile was flushed under DENY_ALLOCATIONS_IN_SCOPE; "
           "the memory-exceeded flush path is not guarded";
}

#endif // MEMORY_TRACKER_DEBUG_CHECKS
#endif // USE_JEMALLOC
