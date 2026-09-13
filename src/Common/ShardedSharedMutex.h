#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <mutex>
#include <thread>

#include <base/defines.h>
#include <base/types.h>
#include <Common/CacheLine.h>

namespace DB
{

/** A shared mutex for read-mostly state that is read by many threads at once.
  *
  * `SharedMutex::lock_shared` increments a single atomic word, so every reader
  * performs a compare-exchange on one cache line. That line then migrates
  * between all the cores taking the lock, and the cost grows with the number of
  * readers even though they never conflict with each other. In the query
  * pipeline executor this showed up as ~23% of a wide pipeline passing small
  * blocks, and it is why such a pipeline gets *slower* past a few threads.
  *
  * Here each reader instead touches its own cache line, so readers do not
  * contend at all, and the only shared state on the read path is a flag that
  * stays in every core's cache in shared state as long as no writer appears.
  *
  * The trade is that a writer must scan every shard, so this is only worth it
  * when writes are rare. Correctness is the usual store-then-load pattern in
  * both directions: a reader publishes its presence and then checks for a
  * writer, a writer publishes itself and then checks for readers, and
  * sequential consistency on those four operations guarantees at least one of
  * them observes the other.
  *
  * NOT recursive: taking the shared lock twice in one thread can deadlock
  * against a waiting writer, exactly as it can with a fair shared mutex.
  */
class TSA_CAPABILITY("ShardedSharedMutex") ShardedSharedMutex
{
public:
    ShardedSharedMutex() = default;
    ShardedSharedMutex(const ShardedSharedMutex &) = delete;
    ShardedSharedMutex & operator=(const ShardedSharedMutex &) = delete;

    void lock_shared() TSA_ACQUIRE_SHARED()
    {
        Shard & shard = shards[shardIndex()];
        while (true)
        {
            shard.readers.fetch_add(1, std::memory_order_seq_cst);
            if (!writer_active.load(std::memory_order_seq_cst)) [[likely]]
                return;

            /// A writer is here or on its way; stand back so it can drain.
            shard.readers.fetch_sub(1, std::memory_order_seq_cst);
            while (writer_active.load(std::memory_order_acquire))
                spinPause();
        }
    }

    void unlock_shared() TSA_RELEASE_SHARED()
    {
        shards[shardIndex()].readers.fetch_sub(1, std::memory_order_release);
    }

    void lock() TSA_ACQUIRE()
    {
        while (writer_lock.test_and_set(std::memory_order_acquire))
            spinPause();
        writer_active.store(true, std::memory_order_seq_cst);
        for (auto & shard : shards)
            while (shard.readers.load(std::memory_order_seq_cst) != 0)
                spinPause();
    }

    void unlock() TSA_RELEASE()
    {
        writer_active.store(false, std::memory_order_release);
        writer_lock.clear(std::memory_order_release);
    }

private:
    struct alignas(CH_CACHE_LINE_SIZE) Shard
    {
        std::atomic<Int64> readers{0};
    };

    /// Enough that the threads of a query pipeline rarely collide; collisions
    /// only cost contention, never correctness.
    static constexpr size_t NUM_SHARDS = 64;

    static size_t shardIndex()
    {
        /// Assigned once per thread, so a thread always releases on the shard
        /// it acquired on, and pool threads keep a stable slot.
        static std::atomic<size_t> next_index{0};
        thread_local const size_t index = next_index.fetch_add(1, std::memory_order_relaxed) % NUM_SHARDS;
        return index;
    }

    static void spinPause()
    {
#if defined(__x86_64__)
        __builtin_ia32_pause();
#elif defined(__aarch64__)
        __asm__ __volatile__("isb" ::: "memory");
#endif
    }

    std::array<Shard, NUM_SHARDS> shards;
    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> writer_active{false};
    /// Writer-vs-writer exclusion. A plain flag rather than a std::mutex so
    /// that acquiring in lock() and releasing in unlock() does not trip the
    /// static thread-safety analyser, which cannot follow a lock across
    /// function boundaries. Writers are rare, so a spin is fine here.
    std::atomic_flag writer_lock = ATOMIC_FLAG_INIT;
};

}
