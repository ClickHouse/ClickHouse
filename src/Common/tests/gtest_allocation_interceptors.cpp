#if !defined(SANITIZER)

#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

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

    auto before_thread = CurrentThread::get().memory_tracker.get();
    auto before_global = total_memory_tracker.get();

    while (before_thread > 0 || before_global > 0)
    {
        before_thread = CurrentThread::get().memory_tracker.get();
        before_global = total_memory_tracker.get();
    }

    auto ptr = allocation_callback();

    /// The MemoryTracker uses atomics with std::memory_order_relaxed
    /// so we check the condition in a loop to ensure we don't see stale values.
    bool allocation_ok = false;
    Int64 after_thread = 0;
    Int64 after_global = 0;
    for (size_t i = 0; i < 1000 && !allocation_ok; ++i)
    {
        after_thread = CurrentThread::get().memory_tracker.get();
        after_global = total_memory_tracker.get();
        /// No double accounting
        bool thread_ok = (after_thread - before_thread) >= allocation_size &&
                         (after_thread - before_thread) <= allocation_size * 1.1;
        bool global_ok = (after_global - before_global) >= allocation_size &&
                         (after_global - before_global) <= allocation_size * 1.1;
        allocation_ok |= thread_ok & global_ok;
    }

    ASSERT_TRUE(allocation_ok);

    deallocation_callback(ptr);

    bool deallocation_ok = false;
    for (size_t i = 0; i < 1000 && !deallocation_ok; ++i)
    {
        /// We might have allocated something else on our way. But this amount should be negligible.
        /// Also, no double accounting.
        bool thread_ok = (after_thread - CurrentThread::get().memory_tracker.get()) >= allocation_size * 0.95 &&
                         (after_thread - CurrentThread::get().memory_tracker.get()) <= allocation_size * 1.1;
        bool global_ok = (after_global - total_memory_tracker.get()) >= allocation_size * 0.95 &&
                         (after_global - total_memory_tracker.get()) <= allocation_size * 1.1;
        deallocation_ok |= thread_ok & global_ok;
    }
    ASSERT_TRUE(deallocation_ok);
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

/// NOLINTEND

/// Write more tests if needed.

#endif
