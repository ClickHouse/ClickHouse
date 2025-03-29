#pragma once

#include <condition_variable>
#include <shared_mutex>
#include <Common/ThreadPool.h>
#include <Common/futex.h>

namespace DB::Parquet
{

struct ReadOptions
{
    bool use_bloom_filter = true;
    bool use_page_index = true;
    bool use_prewhere = true;
    bool seekable_read = true;
    bool use_prefetch = true;
    size_t min_bytes_for_seek = 64 << 10;
    size_t bytes_per_read_task = 4 << 20;
    /// Currently only affects the case where dictionary encoding has very high compression ratio.
    /// Outside that case, we always decode and deliver whole row groups at once.
    size_t preferred_block_size_bytes = 256 << 20;
};


/// TODO: Move this to a shared file and reconcile with https://github.com/ClickHouse/ClickHouse/pull/66253/files , probably make it opaque and created by FormatFactory or something, or contains an opaque shared_ptr lazy-initialized by the format
struct SharedParsingThreadPool
{
    std::optional<ThreadPool> io_pool;
    std::optional<ThreadPool> parsing_pool;

    size_t total_memory_target;
    std::atomic<size_t> num_readers {0};

    /// If a reader's memory usage is below this, the reader should schedule more tasks.
    size_t getMemoryTargetPerReader() const { return total_memory_target / num_readers.load(std::memory_order_relaxed); }
};

using SharedParsingThreadPoolPtr = std::shared_ptr<SharedParsingThreadPool>;


/// Usage:
///
/// struct Foo
/// {
///     std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();
///
///     ~Foo()
///     {
///         shutdown->shutdown();
///
///         // No running background tasks remain.
///         // If more background tasks are still scheduled in thread pools, these tasks will detect
///         // shutdown and return early without accessing `this`.
///     }
///
///     void someBackgroundTask(std::shared_ptr<ShutdownHelper> shutdown_)
///     {
///         std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
///         if (!shutdown_lock.owns_lock())
///             return; // shutdown was requested, `this` may be destroyed
///
///         // `this` is safe to access as long as `shutdown_lock` is held.
///     }
/// }
///
/// Fun fact: ShutdownHelper can almost be replaced with std::shared_mutex.
/// Background tasks would do try_lock_shared(). Shutdown would do lock() and never unlock.
/// Alas, std::shared_mutex::try_lock_shared() is allowed to spuriously fail, so this doesn't work.
/// (In Common/SharedMutex.h, the futex-based implementation has reliable try_lock_shared(), but the
/// fallback absl implementation can fail spuriously. In Common/CancelableSharedMutex.h there's
/// another suitable futex-based linux-only implementation.)
class ShutdownHelper
{
public:
    bool try_lock_shared()
    {
        Int64 n = val.fetch_add(1, std::memory_order_acquire) + 1;
        chassert(n != SHUTDOWN_START);
        if (n >= SHUTDOWN_START)
        {
            unlock_shared();
            return false;
        }
        return true;
    }
    void unlock_shared()
    {
        Int64 n = val.fetch_sub(1, std::memory_order_release) - 1;
        chassert(n >= 0);
        if (n == SHUTDOWN_START)
        {
            /// We're the last completed task. Add SHUTDOWN_END to indicate that no further waiting
            /// or cv notifying is needed, even though `val` can get briefly bumped up and down by
            /// unsuccessful try_lock_shared() calls.
            val.fetch_add(SHUTDOWN_END);
            {
                /// Lock and unlock the mutex. This may look weird, but this is usually (always?)
                /// required to avoid race conditions when combining condition_variable with atomics.
                ///
                /// In this case, the prevented race condition is:
                ///  1. unlock_shared() sees n == SHUTDOWN_START,
                ///  2. shutdown thread enters cv.wait(lock, [&] { return val.load() >= SHUTDOWN_END; });
                ///     the callback does val.load(), gets SHUTDOWN_START, and is about
                ///     to return false; at this point, the cv.wait call is not monitoring
                ///     condition_variable notifications (remember that cv.wait with callback is
                ///     equivalent to a wait without callback in a loop),
                ///  3. the unlock_shared() assigns `val` and calls cv.notify_all(), which does
                ///     nothing because no thread is blocked on the condition variable,
                ///  4. the cv.wait callback returns false; the wait goes back to sleep and never
                ///     wakes up.
                std::unique_lock lock(mutex);
            }
            cv.notify_all();
        }
    }

    /// For re-checking in the middle of long-running operation while already holding a lock.
    bool shutdown_requested()
    {
        return val.load(std::memory_order_relaxed) >= SHUTDOWN_START;
    }

    void begin_shutdown()
    {
        Int64 n = val.fetch_add(SHUTDOWN_START) + SHUTDOWN_START;
        chassert(n < SHUTDOWN_START * 2); // shutdown requested more than once
        if (n == SHUTDOWN_START)
            val.fetch_add(SHUTDOWN_END);
    }
    void wait_shutdown()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return val.load() >= SHUTDOWN_END; });
    }

    void shutdown()
    {
        begin_shutdown();
        wait_shutdown();
    }

private:
    static constexpr Int64 SHUTDOWN_START = 1l << 42; // shutdown requested
    static constexpr Int64 SHUTDOWN_END = 1l << 52; // no shared locks remain

    /// If >= SHUTDOWN_START, no new try_lock_shared() calls will succeed.
    /// Whoever changes the value to exactly SHUTDOWN_START (i.e. shutdown requested, no shared locks)
    /// must then add SHUTDOWN_END to it.
    std::atomic<Int64> val {0};
    std::mutex mutex;
    std::condition_variable cv;
};


#ifdef OS_LINUX

class CompletionNotification
{
private:
    enum State : UInt32
    {
        EMPTY,
        WAITING,
        NOTIFIED,
    };

    std::atomic<UInt32> val {0};

public:
    bool check() const
    {
        return val.load(std::memory_order_acquire) == NOTIFIED;
    }

    void wait()
    {
        UInt32 n = val.load(std::memory_order_acquire);
        if (n == NOTIFIED)
            return;
        if (n == EMPTY)
        {
            if (!val.compare_exchange_strong(n, WAITING))
            {
                chassert(n == NOTIFIED);
                return;
            }
        }
        while (true)
        {
            futexWait(&val, WAITING);
            n = val.load();
            if (n == NOTIFIED)
                return;
            chassert(n == WAITING);
        }
    }

    void notify()
    {
        UInt32 n = val.exchange(NOTIFIED);
        /// If there were no wait() calls before the notify() call, avoid the syscall.
        if (n == WAITING)
            futexWake(&val, INT32_MAX);
    }
};

#else //TODO: test this

class CompletionNotification
{
private:
    std::promise<void> promise;
    std::future<void> future;
    std::atomic<bool> notified {false};

public:
    bool check() const
    {
        return notified.load();
    }

    void wait()
    {
        if (!check())
            future.wait();
    }

    void notify()
    {
        if (!notified.exchange(true))
            promise.set_value();
    }
};

#endif

}
