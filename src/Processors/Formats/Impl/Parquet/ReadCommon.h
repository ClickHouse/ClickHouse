#pragma once

#include <condition_variable>
#include <shared_mutex>
#include <Common/futex.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

namespace DB::Parquet
{

struct ReadOptions
{
    //TODO: make sure all of these are randomized in tests, and also writer settings
    bool use_row_group_min_max = true;
    bool use_bloom_filter = true;
    bool use_page_min_max = true;
    bool always_use_offset_index = true;

    bool seekable_read = true;

    bool case_insensitive_column_matching = false;
    bool schema_inference_force_nullable = false;
    bool schema_inference_force_not_nullable = false;
    bool null_as_default = true;

    bool fuzz = false;//TODO: randomize lots of things when enabled

    /// Use dictionary filter if dictionary page is smaller than this (and all values in the column
    /// chunk are dictionary-encoded). This takes precedence over bloom filter. 0 to disable.
    size_t dictionary_filter_limit_bytes = 0;

    size_t min_bytes_for_seek = 64 << 10;
    size_t bytes_per_read_task = 4 << 20;

    size_t preferred_block_size_bytes = DEFAULT_BLOCK_SIZE * 256;
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
};


//TODO: Move implementations to .cpp file.


/// TODO: Move this to a shared file and reconcile with https://github.com/ClickHouse/ClickHouse/pull/66253/files , probably make it opaque and created by FormatFactory or something, or contains an opaque shared_ptr lazy-initialized by the format
struct SharedParsingThreadPool
{
    ThreadPoolCallbackRunnerFast io_runner;
    ThreadPoolCallbackRunnerFast parsing_runner;

    size_t total_memory_target = 0;
    std::atomic<size_t> num_readers {0};

    /// If a reader's memory usage is below this, the reader should schedule more tasks.
    size_t getMemoryTargetPerReader() const { return total_memory_target / std::max(1ul, num_readers.load(std::memory_order_relaxed)); }
};

using SharedParsingThreadPoolPtr = std::shared_ptr<SharedParsingThreadPool>;


/// Each column chunk goes through some subsequence of these stages, in order.
///
/// The scheduling of all this work (in ReadManager) is pretty complicated.
/// Some of the tasks apply to column chunk (e.g. reading bloom filter), some apply to part of
/// a column chunk ("column subchunk" we call it). Some stages need some per-row-group work after
/// finishing all per-column tasks (e.g. apply KeyCondition after reading bloom filters for all
/// columns).
///
/// Here's a slightly simplified dependency graph:
/// https://github.com/user-attachments/assets/57213f9c-1588-406c-980e-ff0a9ab56e95
/// (if you need to edit this diagram, load this into excalidraw:
///  https://pastila.nl/?cafebabe/5f32c6546f4797c537707535c515f2c3#Fp02Ps7p2hRahC0B5cK+TQ== )
///
/// An important role of this enum is to separately control parallelism of different stages.
/// E.g. typically column index is small, and we can read it in lots of columns and row groups
/// in parallel (especially useful if we're reading over network and are latency-bound).
/// But main data is often big enough that we can't afford enough memory to read many row groups in
/// parallel. We'd like the parallelism to automatically scale based on memory usage.
/// But also we don't want to get into a situation where e.g. most of the memory budget is used by
/// column indexes and there's not enough left to read main data for a few row groups in parallel.
/// To solve these two problems at once, we do memory accounting separately for each stage, with
/// separate memory budget for each stage (see ReadManager::Stage).
/// Memory is attributed to the stage that allocated it. E.g. ReadManager::read() (Deliver stage)
/// may release a column that was allocated by PrewhereData stage, reducing PrewhereData's memory
/// usage and potentially kicking off more PrewhereData read tasks.
enum class ReadStage
{
    NotStarted = 0,
    BloomFilterHeader,
    BloomFilterBlocksOrDictionary,
    ColumnIndexAndOffsetIndex,
    PrewhereOffsetIndex,
    PrewhereData,
    MainOffsetIndex, // "main" means columns that are not in prewhere
    MainData,
    Deliver,
    Deallocated,
};

/// Subsequence of ReadStage-s relevant to a whole RowGroup.
/// Each such stage transition is a barrier synchronizing all columns of the row group.
/// E.g. bloom filters have two stages in ReadStage but one stage here because the two stages
/// (read header, then read blocks) need to happen sequentially within each column; then, after all
/// columns finish both stages, some row-group-level work needs to happen (applying KeyCondition
/// using all columns' bloom filters at once) before any column can proceed to the next stage.
enum class RowGroupReadStage
{
    NotStarted = 0,
    BloomAndDictionaryFilters,
    ColumnIndex,
    Subgroups,
    Deallocated,
};

enum class RowSubgroupReadStage
{
    NotStarted = 0,
    Prewhere,
    MainColumns,
    Deliver,
    Deallocated,
};


/// We track approximate current memory usage per ReadStage that allocated the memory (*).
/// This struct aggregates how much memory was allocated by some operation.
/// ReadManager then uses it to update per-stage memory usage std::atomic counters.
/// (We do this instead of updating the std::atomics directly to reduce contention on the atomics.
///  I haven't checked whether this makes a difference.)
///
/// (*) This is to have a separate memory limit on each stage to automatically get higher parallelism
/// for stages that use little memory (e.g. prefetch small bloom filters and indexes for lots of row
/// groups in parallel, but read large column data for few row groups to not run out of memory).
//TODO: Try using thread-locals instead of manually error-pronely passing this everywhere.
struct MemoryUsageDiff
{
    ReadStage cur_stage;
    std::array<ssize_t, size_t(ReadStage::Deallocated)> by_stage {};
    bool finalized = false;
    /// True if we may have unblocked some tasks by means other then freeing memory
    /// (specifically, we advanced first_incomplete_row_group or delivery_ptr).
    bool retry_scheduling = false;

    explicit MemoryUsageDiff(ReadStage cur_stage_) : cur_stage(cur_stage_) {}
    MemoryUsageDiff() = delete;
    MemoryUsageDiff(const MemoryUsageDiff &) = delete;
    MemoryUsageDiff & operator=(const MemoryUsageDiff &) = delete;

    ~MemoryUsageDiff()
    {
        chassert(finalized || std::uncaught_exceptions() > 0);
    }

    void allocated(size_t amount)
    {
        chassert(cur_stage > ReadStage::NotStarted);
        chassert(cur_stage < ReadStage::Deliver);
        chassert(!finalized);
        by_stage.at(size_t(cur_stage)) += ssize_t(amount);
    }
    void deallocated(size_t amount, ReadStage stage)
    {
        chassert(!finalized);
        by_stage.at(size_t(stage)) -= ssize_t(amount);
    }
};

/// Remembers the ReadStage and size of a memory allocation.
/// Not RAII, you have to call reset to update the stat.
class MemoryUsageToken
{
public:
    MemoryUsageToken() = default;
    MemoryUsageToken(size_t val_, MemoryUsageDiff * diff)
        : alloc_stage(diff->cur_stage), val(val_)
    {
        diff->allocated(val);
    }
    MemoryUsageToken(MemoryUsageToken && rhs) noexcept
    {
        *this = std::move(rhs);
    }
    MemoryUsageToken & operator=(MemoryUsageToken && rhs) noexcept
    {
        chassert(!val);
        alloc_stage = std::exchange(rhs.alloc_stage, ReadStage::Deallocated);
        val = std::exchange(rhs.val, 0);
        return *this;
    }

    operator bool() const { return alloc_stage != ReadStage::Deallocated; }

    void reset(MemoryUsageDiff * diff)
    {
        if (val)
            diff->deallocated(val, alloc_stage);
        val = 0;
        alloc_stage = ReadStage::Deallocated;
    }
    void add(size_t amount, MemoryUsageDiff * diff)
    {
        chassert(diff->cur_stage == alloc_stage);
        diff->allocated(amount);
        val += amount;
    }

private:
    ReadStage alloc_stage = ReadStage::Deallocated;
    size_t val = 0;
};


/*TODO: delet dis
class MemoryUsageToken
{
public:
    explicit MemoryUsageToken(std::atomic<size_t> * ptr_ = nullptr, size_t val_ = 0)
        : ptr(ptr_)
    {
        set(val_);
    }

    MemoryUsageToken(MemoryUsageToken && rhs) noexcept
    {
        *this = std::move(rhs);
    }
    MemoryUsageToken & operator=(MemoryUsageToken && rhs)
    {
        set(0);
        ptr = std::exchange(rhs.ptr, nullptr);
        val = std::exchange(rhs.val, 0);
        return *this;
    }
    ~MemoryUsageToken()
    {
        set(0);
    }

    void set(size_t new_val)
    {
        if (!ptr || new_val == val)
            return;
        ptr->fetch_add(new_val - val, std::memory_order_relaxed); // overflow is ok
        val = new_val;
    }

private:
    std::atomic<size_t> * ptr = nullptr;
    size_t val = 0;
};*/


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
            return; // fast path
        if (n == EMPTY)
        {
            if (!val.compare_exchange_strong(n, WAITING))
            {
                if (n == NOTIFIED)
                    return;
                chassert(n == WAITING);
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


/*TODO: delet dis
/// Simple RW spinlock with writer priority. Cachline padding recommended:
/// alignas(std::hardware_destructive_interference_size) RWSpinLockWriterFirst lock;
class RWSpinlockWriterFirst
{
public:
    void lock_shared()
    {
        lock_impl(1);
    }
    void unlock_shared()
    {
        Int64 n = val.fetch_sub(1, std::memory_order_release);
        chassert(n > 0);
    }
    void lock()
    {
        Int64 n = lock_impl(EXCLUSIVE);

        /// Wait for all shared locks to get unlocked.
        while (n > EXCLUSIVE)
        {
            std::this_thread::yield();
            n = val.load(std::memory_order_acquire);
            chassert(n >= EXCLUSIVE);
        }
    }
    void unlock()
    {
        Int64 n = val.fetch_sub(EXCLUSIVE, std::memory_order_release);
        chassert(n >= EXCLUSIVE);
    }
    bool try_lock_shared()
    {
        return try_lock_impl(1).second;
    }

private:
    static constexpr Int64 EXCLUSIVE = 1l << 42;
    /// val = [number of shared locks] + EXCLUSIVE*[number of exclusive candidate locks].
    std::atomic<Int64> val {0};

    /// For both shared and exclusive locks: if you managed to increase `val`
    /// from some value x < EXCLUSIVE to a value x + something, this means you got the lock.
    std::pair<Int64, bool> try_lock_impl(Int64 amount)
    {
        /// Speculatively add the value in hopes there's no contention.
        Int64 n = val.fetch_add(amount, std::memory_order_acquire);
        chassert(n >= 0);
        if (n < EXCLUSIVE)
            return {n + amount, true}; // fast path
        /// Oops, there's contention. Un-add and let the caller fall back to compare-and-swap loop.
        n = val.fetch_sub(amount, std::memory_order_relaxed) - amount;
        return {n, false};
    }
    Int64 lock_impl(Int64 amount)
    {
        Int64 n;
        bool locked;
        std::tie(n, locked) = try_lock_impl(amount);
        if (locked)
            return n;

        while (true)
        {
            chassert(n >= 0);
            if (n >= EXCLUSIVE)
            {
                std::this_thread::yield();
                n = val.load(std::memory_order_relaxed);
                continue;
            }
            if (val.compare_exchange_weak(n, n + amount, std::memory_order_acquire, std::memory_order_relaxed))
                return n + amount;
        }
    }
};*/

/// Relatively simple multiple-producer multiple-consumer ring buffer.
/// Probably faster than std::queue + std::mutex, probably slower than fancy complicated
/// implementations like folly::MPMCQueue (because of contention on atomics).
//(TODO)

}
