#if !defined(SANITIZER)

#include "config.h"

#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/memory.h>
#include <base/getPageSize.h>
#include <base/scope_guard.h>
#include <cerrno>
#include <cstdlib>
#include <limits>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace ProfileEvents
{
    extern const Event GlobalMemoryLimitExceeded;
    extern const Event QueryMemoryLimitExceeded;
}

using namespace DB;

/// NOLINTBEGIN

static void __attribute__((noinline)) useMisterPointer(void * p)
{
    __asm__ volatile ("" : : "r"(p) : "memory");
}

/// Allocate 64MB which is bigger than max_untracked_memory
static constexpr int allocation_size = 1024 * 1024 * 64;

void checkMemory(auto allocation_callback, auto deallocation_callback)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::flushUntrackedMemory();

    const auto before_thread = CurrentThread::get().memory_tracker.get();
    const auto before_global = total_memory_tracker.get();

    auto ptr = allocation_callback();
    CurrentThread::flushUntrackedMemory();

    const auto after_thread = CurrentThread::get().memory_tracker.get();
    const auto after_global = total_memory_tracker.get();

    /// The allocation should be tracked (no under-counting) without double-accounting.
    ASSERT_GE(after_thread - before_thread, allocation_size);
    ASSERT_LE(static_cast<double>(after_thread - before_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(after_global - before_global, allocation_size);
    ASSERT_LE(static_cast<double>(after_global - before_global), static_cast<double>(allocation_size) * 1.1);

    deallocation_callback(ptr);
    CurrentThread::flushUntrackedMemory();

    const auto freed_thread = after_thread - CurrentThread::get().memory_tracker.get();
    const auto freed_global = after_global - total_memory_tracker.get();

    /// The deallocation should be tracked without double-accounting.
    ASSERT_GE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 0.95);
    ASSERT_LE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 0.95);
    ASSERT_LE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 1.1);
}

TEST(AllocationInterceptors, MallocIncreasesTheMemoryTracker)
{
    checkMemory([&]()
    {
        /// Several tricks to ensure the compiler doesn't optimize the allocation out.
        [[ maybe_unused ]] void * ptr = malloc(allocation_size);
        useMisterPointer(ptr);
        *reinterpret_cast<char *>(ptr) = 'a';
        return ptr;
    }, [&](void * ptr) { free(ptr); });
}

TEST(AllocationInterceptors, NewDeleteIncreasesTheMemoryTracker)
{
    checkMemory([&]()
    {
        /// Several tricks to ensure the compiler doesn't optimize the allocation out.
        [[ maybe_unused ]] char * ptr = new char[allocation_size];
        useMisterPointer(ptr);
        *ptr = 'a';
        return ptr;
    }, [&](const char * ptr) { delete[] ptr; });
}

TEST(AllocationInterceptors, FailedReallocPreservesOldAllocationAccounting)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::flushUntrackedMemory();

    const Int64 before_alloc_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_alloc_global = total_memory_tracker.get();

    void * ptr = malloc(allocation_size);
    ASSERT_NE(ptr, nullptr);
    useMisterPointer(ptr);
    *reinterpret_cast<char *>(ptr) = 'a';
    CurrentThread::flushUntrackedMemory();

    const auto after_alloc_thread = CurrentThread::get().memory_tracker.get();
    const auto after_alloc_global = total_memory_tracker.get();

    ASSERT_GE(after_alloc_thread - before_alloc_thread, allocation_size);
    ASSERT_LE(static_cast<double>(after_alloc_thread - before_alloc_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(after_alloc_global - before_alloc_global, allocation_size);
    ASSERT_LE(static_cast<double>(after_alloc_global - before_alloc_global), static_cast<double>(allocation_size) * 1.1);

    /// A failed realloc must not lose the old block's accounting.
    void * failed_realloc = realloc(ptr, std::numeric_limits<size_t>::max());
    ASSERT_EQ(failed_realloc, nullptr);
    CurrentThread::flushUntrackedMemory();

    const auto thread_after_realloc = CurrentThread::get().memory_tracker.get();
    const auto global_after_realloc = total_memory_tracker.get();

    EXPECT_GE(static_cast<double>(thread_after_realloc), static_cast<double>(after_alloc_thread) * 0.95);
    EXPECT_LE(static_cast<double>(thread_after_realloc), static_cast<double>(after_alloc_thread) * 1.1);
    EXPECT_GE(static_cast<double>(global_after_realloc), static_cast<double>(after_alloc_global) * 0.95);
    EXPECT_LE(static_cast<double>(global_after_realloc), static_cast<double>(after_alloc_global) * 1.1);

    free(ptr);
    CurrentThread::flushUntrackedMemory();

    const auto freed_thread = after_alloc_thread - CurrentThread::get().memory_tracker.get();
    const auto freed_global = after_alloc_global - total_memory_tracker.get();

    EXPECT_GE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 0.95);
    EXPECT_LE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 1.1);
    EXPECT_GE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 0.95);
    EXPECT_LE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 1.1);
}

TEST(AllocationInterceptors, MallocZeroFreeDoesNotCauseNegativeDrift)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    const Int64 before_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_global = total_memory_tracker.get();

    constexpr size_t iterations = 100000;
    for (size_t i = 0; i < iterations; ++i)
    {
        void * ptr = malloc(0);
        free(ptr);
    }

    EXPECT_GE(CurrentThread::get().memory_tracker.get() - before_thread, -64 * 1024);
    EXPECT_GE(total_memory_tracker.get() - before_global, -64 * 1024);
}

namespace
{

/// Restores the global hard limit and the operator-new throw threshold on scope exit.
struct MemoryLimitGuard
{
    Int64 prev_hard_limit;
    /// Threshold setter takes UInt64; cache nothing — restore to 0 (default = disabled).
    MemoryLimitGuard()
        : prev_hard_limit(total_memory_tracker.getHardLimit())
    {
    }
    ~MemoryLimitGuard()
    {
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(0);
        total_memory_tracker.setHardLimit(prev_hard_limit);
    }
};

/// Set hard limit just above the current amount so the next big allocation overshoots.
void clampHardLimitJustAboveCurrent()
{
    MainThreadStatus::getInstance();
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.setHardLimit(total_memory_tracker.get() + 1024);
}

}

TEST(AllocationInterceptors, MinAllocSizeToThrowDisabledDoesNotRefuseLargeNew)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(0);
    clampHardLimitJustAboveCurrent();

    char * ptr = new char[allocation_size];
    useMisterPointer(ptr);
    *ptr = 'a';
    delete[] ptr;
}

TEST(AllocationInterceptors, MinAllocSizeToThrowRefusesLargeNewPastLimit)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 20);
    clampHardLimitJustAboveCurrent();

    EXPECT_THROW({
        char * ptr = new char[allocation_size];
        useMisterPointer(ptr);
    }, DB::Exception);
}

TEST(AllocationInterceptors, MinAllocSizeToThrowDoesNotAffectExplicitAllocPath)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 30);
    clampHardLimitJustAboveCurrent();

    EXPECT_THROW({
        std::ignore = CurrentMemoryTracker::alloc(allocation_size);
    }, DB::Exception);
}

TEST(AllocationInterceptors, GlobalMemoryLimitExceededOnlyIncrementsForGlobalTracker)
{
    MainThreadStatus::getInstance();

    MemoryLimitGuard limit_guard;
    auto & thread_tracker = CurrentThread::get().memory_tracker;
    MemoryTracker query_tracker(&total_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor=*/ false);
    MemoryTracker * prev_parent = thread_tracker.getParent();
    Int64 prev_hard_limit = thread_tracker.getHardLimit();
    const auto reset_test_counters = [&]
    {
        total_memory_tracker.resetCounters();
        thread_tracker.resetCounters();
        query_tracker.resetCounters();
        CurrentThread::getProfileEvents().resetCounters();
    };

    CurrentThread::flushUntrackedMemory();

    SCOPE_EXIT({
        CurrentThread::flushUntrackedMemory();
        reset_test_counters();
        thread_tracker.setHardLimit(prev_hard_limit);
        thread_tracker.setParent(prev_parent);
    });

    thread_tracker.setParent(&query_tracker);

    reset_test_counters();

    query_tracker.setHardLimit(query_tracker.get() + 1024);
    EXPECT_THROW({
        std::ignore = CurrentMemoryTracker::alloc(allocation_size);
    }, DB::Exception);
    EXPECT_EQ(1, CurrentThread::getProfileEvents()[ProfileEvents::QueryMemoryLimitExceeded]);
    EXPECT_EQ(0, CurrentThread::getProfileEvents()[ProfileEvents::GlobalMemoryLimitExceeded]);

    reset_test_counters();

    total_memory_tracker.setHardLimit(total_memory_tracker.get() + 1024);
    EXPECT_THROW({
        std::ignore = CurrentMemoryTracker::alloc(allocation_size);
    }, DB::Exception);
    EXPECT_EQ(1, CurrentThread::getProfileEvents()[ProfileEvents::QueryMemoryLimitExceeded]);
    EXPECT_EQ(1, CurrentThread::getProfileEvents()[ProfileEvents::GlobalMemoryLimitExceeded]);
}

TEST(AllocationInterceptors, MinAllocSizeToThrowDoesNotAffectMalloc)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 20);
    clampHardLimitJustAboveCurrent();

    void * ptr = malloc(allocation_size);
    useMisterPointer(ptr);
    EXPECT_NE(ptr, nullptr);
    *reinterpret_cast<char *>(ptr) = 'a';
    free(ptr);
}

TEST(AllocationInterceptors, MinAllocSizeToThrowDoesNotAffectNoThrowNew)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 20);
    clampHardLimitJustAboveCurrent();

    char * ptr = new (std::nothrow) char[allocation_size];
    EXPECT_NE(ptr, nullptr);
    useMisterPointer(ptr);
    if (ptr)
        *ptr = 'a';
    delete[] ptr;
}

TEST(AllocationInterceptors, MinAllocSizeToThrowRespectsLockMemoryExceptionInThread)
{
    MemoryLimitGuard guard;

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 20);
    clampHardLimitJustAboveCurrent();

    LockMemoryExceptionInThread block(VariableContext::Global);
    char * ptr = new char[allocation_size];
    useMisterPointer(ptr);
    *ptr = 'a';
    delete[] ptr;
}

/// The strict aligned_alloc override lives in src/Common/malloc.cpp only under this condition;
/// without it glibc's lenient aligned_alloc(4096, 47808) succeeds and these assertions do not hold.
#if USE_JEMALLOC && (defined(OS_LINUX) || defined(OS_FREEBSD))

/// Our aligned_alloc override (src/Common/malloc.cpp) strictly enforces the C11 rule that size
/// be a multiple of alignment, unlike glibc's lenient implementation. ThreadStack::getSize() can
/// produce a non-page-aligned size (MINSIGSTKSZ on glibc >= 2.34 is a runtime sysconf value, e.g.
/// 47808 on Sapphire Rapids/AMX), which used to abort startup via this override (issue #108811).
TEST(AllocationInterceptors, AlignedAllocRejectsNonMultipleOfAlignment)
{
    const size_t page_size = getPageSize();
    /// A size between 32 KiB and the next page boundary, not a multiple of the page size.
    const size_t unaligned_size = 47808;
    ASSERT_NE(unaligned_size % page_size, 0u);

    errno = 0;
    void * ptr = aligned_alloc(page_size, unaligned_size);
    EXPECT_EQ(ptr, nullptr);
    EXPECT_EQ(errno, EINVAL);
    free(ptr);
}

TEST(AllocationInterceptors, AlignedAllocAcceptsPageRoundedSize)
{
    const size_t page_size = getPageSize();
    /// The fix rounds the requested size up to a page multiple before allocating.
    const size_t rounded_size = ::Memory::alignUp(static_cast<size_t>(47808), page_size);
    ASSERT_EQ(rounded_size % page_size, 0u);

    void * ptr = aligned_alloc(page_size, rounded_size);
    ASSERT_NE(ptr, nullptr);
    useMisterPointer(ptr);
    free(ptr);
}

#endif // USE_JEMALLOC && (OS_LINUX || OS_FREEBSD)

/// NOLINTEND

#endif
