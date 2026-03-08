#if !defined(SANITIZER)

#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <limits>
#include <netdb.h>
#include <sys/socket.h>

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
                         static_cast<double>(after_thread - before_thread) <= static_cast<double>(allocation_size) * 1.1;
        bool global_ok = (after_global - before_global) >= allocation_size &&
                         static_cast<double>(after_global - before_global) <= static_cast<double>(allocation_size) * 1.1;
        allocation_ok |= thread_ok & global_ok;
    }

    ASSERT_TRUE(allocation_ok);

    deallocation_callback(ptr);

    bool deallocation_ok = false;
    for (size_t i = 0; i < 1000 && !deallocation_ok; ++i)
    {
        /// We might have allocated something else on our way. But this amount should be negligible.
        /// Also, no double accounting.
        bool thread_ok
            = static_cast<double>(after_thread - CurrentThread::get().memory_tracker.get()) >= static_cast<double>(allocation_size) * 0.95
            && static_cast<double>(after_thread - CurrentThread::get().memory_tracker.get()) <= static_cast<double>(allocation_size) * 1.1;
        bool global_ok = static_cast<double>(after_global - total_memory_tracker.get()) >= static_cast<double>(allocation_size) * 0.95
            && static_cast<double>(after_global - total_memory_tracker.get()) <= static_cast<double>(allocation_size) * 1.1;
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

TEST(AllocationInterceptors, FailedReallocPreservesOldAllocationAccounting)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    const Int64 before_alloc_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_alloc_global = total_memory_tracker.get();

    void * ptr = malloc(allocation_size);
    ASSERT_NE(ptr, nullptr);
    useMisterPointer(ptr);
    *reinterpret_cast<char *>(ptr) = 'a';

    bool allocation_ok = false;
    Int64 after_alloc_thread = 0;
    Int64 after_alloc_global = 0;
    for (size_t i = 0; i < 1000 && !allocation_ok; ++i)
    {
        after_alloc_thread = CurrentThread::get().memory_tracker.get();
        after_alloc_global = total_memory_tracker.get();
        bool thread_ok = (after_alloc_thread - before_alloc_thread) >= allocation_size
            && static_cast<double>(after_alloc_thread - before_alloc_thread) <= static_cast<double>(allocation_size) * 1.1;
        bool global_ok = (after_alloc_global - before_alloc_global) >= allocation_size
            && static_cast<double>(after_alloc_global - before_alloc_global) <= static_cast<double>(allocation_size) * 1.1;
        allocation_ok |= thread_ok & global_ok;
    }
    ASSERT_TRUE(allocation_ok);

    void * failed_realloc = realloc(ptr, std::numeric_limits<size_t>::max());
    ASSERT_EQ(failed_realloc, nullptr);

    bool realloc_ok = false;
    for (size_t i = 0; i < 1000 && !realloc_ok; ++i)
    {
        const auto thread_amount = CurrentThread::get().memory_tracker.get();
        const auto global_amount = total_memory_tracker.get();
        bool thread_ok = static_cast<double>(thread_amount) >= static_cast<double>(after_alloc_thread) * 0.95
            && static_cast<double>(thread_amount) <= static_cast<double>(after_alloc_thread) * 1.1;
        bool global_ok = static_cast<double>(global_amount) >= static_cast<double>(after_alloc_global) * 0.95
            && static_cast<double>(global_amount) <= static_cast<double>(after_alloc_global) * 1.1;
        realloc_ok |= thread_ok & global_ok;
    }
    EXPECT_TRUE(realloc_ok);

    free(ptr);

    bool deallocation_ok = false;
    for (size_t i = 0; i < 1000 && !deallocation_ok; ++i)
    {
        bool thread_ok
            = static_cast<double>(after_alloc_thread - CurrentThread::get().memory_tracker.get()) >= static_cast<double>(allocation_size) * 0.95
            && static_cast<double>(after_alloc_thread - CurrentThread::get().memory_tracker.get()) <= static_cast<double>(allocation_size) * 1.1;
        bool global_ok = static_cast<double>(after_alloc_global - total_memory_tracker.get()) >= static_cast<double>(allocation_size) * 0.95
            && static_cast<double>(after_alloc_global - total_memory_tracker.get()) <= static_cast<double>(allocation_size) * 1.1;
        deallocation_ok |= thread_ok & global_ok;
    }
    EXPECT_TRUE(deallocation_ok);
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

TEST(AllocationInterceptors, GetAddrInfoFreeAddrInfoDoesNotCauseNegativeDrift)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    const Int64 before_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_global = total_memory_tracker.get();

    struct addrinfo hints
    {
    };
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;

    struct addrinfo * result = nullptr;
    const int res = getaddrinfo("127.0.0.1", "9000", &hints, &result);
    ASSERT_EQ(res, 0) << gai_strerror(res);
    ASSERT_NE(result, nullptr);

    bool allocation_seen = false;
    Int64 after_thread = before_thread;
    Int64 after_global = before_global;
    for (size_t i = 0; i < 1000 && !allocation_seen; ++i)
    {
        after_thread = CurrentThread::get().memory_tracker.get();
        after_global = total_memory_tracker.get();
        allocation_seen = after_thread > before_thread && after_global > before_global;
    }
    ASSERT_TRUE(allocation_seen);

    freeaddrinfo(result);

    bool deallocation_ok = false;
    for (size_t i = 0; i < 1000 && !deallocation_ok; ++i)
    {
        const auto thread_after_free = CurrentThread::get().memory_tracker.get();
        const auto global_after_free = total_memory_tracker.get();
        bool thread_ok = thread_after_free - before_thread >= -64 * 1024;
        bool global_ok = global_after_free - before_global >= -64 * 1024;
        deallocation_ok = thread_ok && global_ok;
    }

    EXPECT_TRUE(deallocation_ok);
}

/// NOLINTEND

/// Write more tests if needed.

#endif
