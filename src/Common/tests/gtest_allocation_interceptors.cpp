#if !defined(SANITIZER)

#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <limits>

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

/// NOLINTEND

#endif
