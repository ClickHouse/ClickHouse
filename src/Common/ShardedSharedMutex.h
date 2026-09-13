#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <cstddef>
#include <mutex>
#include <thread>

#include <base/defines.h>
#include <base/types.h>
#include <Common/CacheLine.h>
#include <Common/PerCPU.h>

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
  * The number of shards follows the machine's CPU count, so that the executor
  * threads of one query get a shard each instead of colliding a few at a time.
  * It is bounded because one of these lives per query pipeline, and each shard
  * costs a cache line.
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
    ShardedSharedMutex()
        : num_shards(chooseShardCount())
        , shards(std::make_unique<Shard[]>(num_shards))
    {
    }

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

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true)
    {
        Shard & shard = shards[shardIndex()];
        shard.readers.fetch_add(1, std::memory_order_seq_cst);
        if (!writer_active.load(std::memory_order_seq_cst)) [[likely]]
            return true;
        shard.readers.fetch_sub(1, std::memory_order_seq_cst);
        return false;
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
        for (size_t i = 0; i < num_shards; ++i)
            while (shards[i].readers.load(std::memory_order_seq_cst) != 0)
                spinPause();
    }

    bool try_lock() TSA_TRY_ACQUIRE(true)
    {
        if (writer_lock.test_and_set(std::memory_order_acquire))
            return false;

        writer_active.store(true, std::memory_order_seq_cst);
        for (size_t i = 0; i < num_shards; ++i)
        {
            if (shards[i].readers.load(std::memory_order_seq_cst) != 0)
            {
                /// A reader is inside and try_lock must not wait for it, so back
                /// out. Readers that saw the flag in the meantime simply retry.
                writer_active.store(false, std::memory_order_release);
                writer_lock.clear(std::memory_order_release);
                return false;
            }
        }
        return true;
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

    /// One shard per CPU, so a query whose threads fill the machine gets a
    /// shard each. Bounded below so tiny machines still spread a little, and
    /// above so that a very large machine does not make every query pipeline
    /// pay for it (NUM_SHARDS cache lines per instance).
    static size_t chooseShardCount()
    {
        return std::clamp<size_t>(PerCPU::getNumCPUs(), 8, 256);
    }

    /// A stable per-thread slot, so a thread always releases on the shard it
    /// acquired on (which is why this cannot key off the current CPU: a thread
    /// may migrate between lock_shared and unlock_shared) and pool threads keep
    /// the same slot. Reduced modulo this instance's shard count at use.
    static size_t threadSlot()
    {
        static std::atomic<size_t> next_slot{0};
        thread_local const size_t slot = next_slot.fetch_add(1, std::memory_order_relaxed);
        return slot;
    }

    size_t shardIndex() const { return threadSlot() % num_shards; }

    static void spinPause()
    {
#if defined(__x86_64__)
        __builtin_ia32_pause();
#elif defined(__aarch64__)
        __asm__ __volatile__("isb" ::: "memory");
#endif
    }

    const size_t num_shards;
    std::unique_ptr<Shard[]> shards;

    /// The only shared state a reader touches besides its own shard, so it gets
    /// a line to itself and stays resident in every core's cache while no
    /// writer is about.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> writer_active{false};
    /// Writer-vs-writer exclusion. A plain flag rather than a std::mutex so
    /// that acquiring in lock() and releasing in unlock() does not trip the
    /// static thread-safety analyser, which cannot follow a lock across
    /// function boundaries. Writers are rare, so a spin is fine here.
    ///
    /// On its own line as well: it is written by every lock()/unlock(), and
    /// sharing a line with `writer_active` would dirty the line readers poll.
    alignas(CH_CACHE_LINE_SIZE) std::atomic_flag writer_lock;
};

}
