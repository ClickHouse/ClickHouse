#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

using namespace DB;

static void __attribute__((noinline)) useMisterPointer(void * p)
{
    __asm__ volatile ("" : : "r"(p) : "memory");
}

/// Allocate 64KB which is bigger than max_untracked_memory
static constexpr int allocation_size = 1024 * 1024 * 64;

void checkMemory(auto allocation_callback)
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
    for (size_t i = 0; i < 1000; ++i)
    {
        after_thread = CurrentThread::get().memory_tracker.get();
        after_global = total_memory_tracker.get();
        bool thread_ok = (after_thread - before_thread) >= allocation_size;
        bool global_ok = (after_global - before_global) >= allocation_size;
        allocation_ok |= thread_ok & global_ok;
    }
    ASSERT_TRUE(allocation_ok);

    free(ptr);

    bool deallocation_ok = false;
    for (size_t i = 0; i < 1000; ++i)
    {
        /// We might have allocated something else on our way. But this amount should be negligible.
        bool thread_ok = (after_thread - CurrentThread::get().memory_tracker.get()) >= allocation_size * 0.95;
        bool global_ok = (after_global - total_memory_tracker.get()) >= allocation_size * 0.95;
        deallocation_ok |= thread_ok & global_ok;
    }
    ASSERT_TRUE(deallocation_ok);
}

TEST(AllocationInterceptors, MallocIncresesTheMemoryTracker)
{
    checkMemory([&]()
    {
        /// Several tricks to ensure the compiler doesn't optimize the allocation out.
        [[ maybe_unused ]] void * ptr = malloc(allocation_size);
        useMisterPointer(ptr);
        *reinterpret_cast<char *>(ptr) = 'a';
        return ptr;
    });
}

/// Write more tests if needed.
