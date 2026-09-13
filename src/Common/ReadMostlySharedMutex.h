#pragma once

#include <atomic>

#include <base/defines.h>
#include <base/types.h>
#include <Common/CacheLine.h>

namespace DB
{

/** A shared mutex for state that is read by many threads at once and written
  * rarely and briefly.
  *
  * `SharedMutex::lock_shared` takes the read lock with a compare-exchange loop
  * on a single word. Under many concurrent readers most of those attempts fail
  * and retry, and the retries are themselves contention, so the read path gets
  * worse as readers are added rather than merely staying flat. In the query
  * pipeline executor this showed up as ~23% of a wide pipeline passing small
  * blocks, and it is why such a pipeline got *slower* past a few threads.
  *
  * Here a reader publishes itself with an unconditional `fetch_add`, which
  * cannot fail and so cannot retry, and only then checks whether a writer is
  * about; if one is, it withdraws and waits. The counter is still a single
  * contended line, but the hardware queues the increments instead of letting
  * readers fight over a CAS. Measured on a 192-core machine this is worth
  * 1.5x to 6x over `SharedMutex` on the executor's node lock, and sharding the
  * counter across cache lines on top of that was worth nothing at all -- the
  * retrying, not the sharing, was the cost.
  *
  * Correctness is the usual store-then-load pattern in both directions: a
  * reader publishes itself and then looks for a writer, a writer publishes
  * itself and then looks for readers, and sequential consistency on those four
  * operations guarantees at least one of them sees the other.
  *
  * Waiting is by spinning, so this is only suitable where writers are both rare
  * and short; a reader will burn CPU for as long as a writer holds the lock.
  *
  * NOT recursive: taking the shared lock twice in one thread can deadlock
  * against a waiting writer, exactly as it can with a fair shared mutex.
  */
class TSA_CAPABILITY("ReadMostlySharedMutex") ReadMostlySharedMutex
{
public:
    ReadMostlySharedMutex() = default;
    ReadMostlySharedMutex(const ReadMostlySharedMutex &) = delete;
    ReadMostlySharedMutex & operator=(const ReadMostlySharedMutex &) = delete;

    void lock_shared() TSA_ACQUIRE_SHARED()
    {
        while (true)
        {
            readers.fetch_add(1, std::memory_order_seq_cst);
            if (!writer_active.load(std::memory_order_seq_cst)) [[likely]]
                return;

            /// A writer is here or on its way; stand back so it can drain.
            readers.fetch_sub(1, std::memory_order_seq_cst);
            while (writer_active.load(std::memory_order_acquire))
                spinPause();
        }
    }

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true)
    {
        readers.fetch_add(1, std::memory_order_seq_cst);
        if (!writer_active.load(std::memory_order_seq_cst)) [[likely]]
            return true;
        readers.fetch_sub(1, std::memory_order_seq_cst);
        return false;
    }

    void unlock_shared() TSA_RELEASE_SHARED()
    {
        readers.fetch_sub(1, std::memory_order_release);
    }

    void lock() TSA_ACQUIRE()
    {
        while (writer_lock.test_and_set(std::memory_order_acquire))
            spinPause();
        writer_active.store(true, std::memory_order_seq_cst);
        while (readers.load(std::memory_order_seq_cst) != 0)
            spinPause();
    }

    bool try_lock() TSA_TRY_ACQUIRE(true)
    {
        if (writer_lock.test_and_set(std::memory_order_acquire))
            return false;

        writer_active.store(true, std::memory_order_seq_cst);
        if (readers.load(std::memory_order_seq_cst) != 0)
        {
            /// A reader is inside and try_lock must not wait for it, so back
            /// out. Readers that saw the flag meanwhile simply retry.
            writer_active.store(false, std::memory_order_release);
            writer_lock.clear(std::memory_order_release);
            return false;
        }
        return true;
    }

    void unlock() TSA_RELEASE()
    {
        writer_active.store(false, std::memory_order_release);
        writer_lock.clear(std::memory_order_release);
    }

private:
    static void spinPause()
    {
#if defined(__x86_64__)
        __builtin_ia32_pause();
#elif defined(__aarch64__)
        __asm__ __volatile__("isb" ::: "memory");
#endif
    }

    /// Three separate lines: readers write `readers` and read `writer_active`,
    /// so sharing a line would make each reader's increment invalidate the line
    /// the others read the flag from. `writer_lock` is written by every
    /// lock()/unlock() and would dirty the line readers poll.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<Int64> readers{0};
    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> writer_active{false};
    alignas(CH_CACHE_LINE_SIZE) std::atomic_flag writer_lock;
};

}
