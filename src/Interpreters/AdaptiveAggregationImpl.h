#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>

#include <Columns/IColumn_fwd.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SharedMutex.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

/// Tuning of the adaptive aggregation.
/// The probe bypass decides after this many post-freeze rows per thread. It must be reachable
/// by every stream: at 64 threads a thread sees 1/64th of the input, so a filtered ~13M-row
/// aggregation still leaves ~200K rows per thread.
constexpr size_t adaptive_bypass_sample_rows = 65'536;
/// Probing the frozen table pays off only while at least one row in this many hits it.
constexpr size_t adaptive_bypass_hit_rate_inverse = 4;
/// The drain reserves a bucket's table after sampling this fraction of its records.
constexpr size_t adaptive_reserve_sample_inverse = 8;
/// Headroom over the sampled insert rate when reserving.
constexpr double adaptive_reserve_headroom = 1.25;
/// Fixed lookahead of the drain's hash prefetch.
constexpr size_t adaptive_drain_prefetch_look_ahead = 16;
/// A thread gives up on freezing once it has consumed this many times the freeze threshold
/// in rows while holding fewer keys than the threshold. High-cardinality streams freeze
/// within a couple of blocks and skewed streams at roughly threshold / (1 - hot share) rows,
/// so only repeat-dominated tables (average multiplicity above the multiple) ever give up.
/// The value balances two bounds: it caps the tolerated hot share at 1 - 1/multiple (a 90%
/// hot key still freezes with a wide margin), and it must fire within the rows one thread
/// sees on a medium table at a wide fan-out (a 64-thread scan of 50M rows gives each thread
/// less than a million rows).
constexpr size_t adaptive_freeze_give_up_row_multiple = 16;

/// The thaw guard for the opposite failure past the freeze. Staged misses are supposed to
/// be rare keys, each staged about once; a uniform mid-cardinality stream instead misses on
/// the same keys over and over, and staging then re-processes the bulk of the stream that
/// ordinary insertion would absorb as cheap in-place updates. The repeat factor of the
/// staged stream is estimated over a shared sparse sample of staged hashes: repeats of a
/// key collapse onto one sample entry across all threads, so the estimate does not depend
/// on how a key's occurrences spread over the threads. Once the factor exceeds the bound,
/// every thread thaws its table and returns to the baseline path. The unit is the delayed
/// record, because staging and draining cost per record (a run of one key collapses into a
/// single value-staged record). Streams that want the freeze stay well below the bound
/// (high-cardinality keys repeat a few times in total, a skewed tail almost never); a
/// stream that crosses it fires after about the bound times the distinct staged keys in
/// records, a small share of a repeat-dominated stream.
constexpr UInt64 adaptive_thaw_sample_mask = 0xFF;
constexpr size_t adaptive_thaw_min_staged_records = 524'288;
constexpr size_t adaptive_thaw_repeat_factor = 12;

/// A drain table is detached and written only once it holds at least this many keys, so the
/// spilled parts stay reasonably sized instead of one tiny file per chunk; the same floor
/// sizes the batch a pressure sweep claims for a producer-local drain.
constexpr size_t adaptive_pressure_spill_min_keys = 1'000'000;
/// The in-flight concurrency budget for detached tables awaiting serialization, across the
/// session (roughly four floor-sized tables). It bounds how much detached work exists at
/// once, not memory exactly: a reservation is corrected upward once the table is built, and
/// `allocatedBytes` cannot see heap owned internally by complex aggregate states. The finish
/// drain ignores the budget because it must leave nothing behind.
constexpr size_t adaptive_pressure_detached_bytes_budget = 256 << 20;

struct AdaptiveAggregationSession
{
    /// The 256 per-bucket chunk backlogs and the locking that keeps publication atomic.
    /// TODO (nihalzp): Consider using a lock-free queue for the backlog, to avoid contention on the mutex.
    class StagedBacklog
    {
    public:
        /// Registers an immutable chunk with every bucket holding a non-empty slice and counts
        /// its records as outstanding. Only finalized chunks reach this point, so publication
        /// increments, draining decrements, and nothing needs a compensating adjustment.
        void publish(const StagedChunkPtr & chunk);

        /// Puts a chunk a pressure sweep spared back on the buckets for the merge-time drain.
        /// Its records are still outstanding, so only the registration is repeated.
        void requeue(const StagedChunkPtr & chunk) { registerChunk(chunk); }

        /// Claims every enqueued chunk and drops the per-bucket registrations at once: each
        /// chunk is then owned by the returned list alone, so it frees the moment its drain
        /// completes and memory comes back chunk by chunk. Whatever producers publish after
        /// the swap waits for the next sweep or for the merge.
        std::vector<StagedChunkPtr> takeAllForPressureDrain();

        /// The bucket's remaining chunks, read without the mutex: production is over by the
        /// time the merge tasks run (the finish barrier ordered every producer's publish
        /// before the merge sources were created), and the chunks deliberately stay put - the
        /// merge emplaces keys that point into their staged bytes, so they must live until the
        /// merged buckets are converted, and the session (owned by every merge source) is
        /// exactly that lifetime.
        const std::vector<StagedChunkPtr> & forMergeBucket(size_t bucket) const { return buckets[bucket].backlog; }

        /// Drains report their actual progress, so a cancelled drain does not discount records
        /// it never touched. Read for logging at the finish, after every producer flushed.
        void recordDrained(size_t records) { undrained_records.fetch_sub(records, std::memory_order_relaxed); }
        size_t undrainedRecords() const { return undrained_records.load(std::memory_order_relaxed); }

        /// Retires a bucket's chunk references after its merge-and-convert completed: the
        /// borrow of staged key bytes ends at conversion. A chunk frees once the last bucket
        /// holding it retires.
        void releaseMergedBucket(size_t bucket);

    private:
        void registerChunk(const StagedChunkPtr & chunk);

        struct Bucket
        {
            /// Guards the backlog list against concurrent appends, and against the swap-out
            /// of a pressure sweep.
            std::mutex mutex;
            /// Chunks holding a non-empty slice for this bucket.
            std::vector<StagedChunkPtr> backlog;
        };

        std::array<Bucket, ADAPTIVE_AGGREGATION_NUM_BUCKETS> buckets;

        /// Makes a chunk's registration in the per-bucket backlogs atomic against a sweep's
        /// collection: a sweep that caught a half-registered chunk would drain all of its
        /// buckets chunk-major, and the publisher would then register the rest for a second,
        /// double-counting drain at merge time. Publishers share the lock (per-bucket mutexes
        /// still order their pushes); only a collecting sweep takes it exclusively.
        SharedMutex registry_mutex;

        std::atomic<size_t> undrained_records{0};
    };

    StagedBacklog backlog;

    /// An empty two-level variant of the query's aggregation method, initialized by the first
    /// thread that freezes. Under memory pressure the production-time sweeps drain staged
    /// records into it early (see `drainStagedChunksUnderMemoryPressure`); it joins the merge
    /// set when it holds data.
    AggregatedDataVariantsPtr early_drain_variants;

    /// Serializes pressure sweeps: one sweeper at a time sheds memory, and a single sweeper
    /// needs no per-bucket coordination; merge-time drains run after the finish barrier and
    /// need none either. Producers over the trigger block on it deliberately - pausing
    /// production is the backpressure that lets the sweep win.
    std::mutex pressure_sweep_mutex;
    /// Reservations of detached-table bytes against `adaptive_pressure_detached_bytes_budget`,
    /// released as their writes finish. Guarded by a mutex with a condition variable so a
    /// producer that cannot reserve waits for a writer instead of staging on into an
    /// unbounded backlog; the wait breaks on cancellation (`cancel` notifies).
    std::mutex detached_spill_mutex;
    std::condition_variable detached_spill_cv;
    size_t estimated_detached_spill_bytes = 0;

    /// The RAII side of the budget: acquire (or wait), correct to the real footprint once the
    /// table is built, release on destruction even if the write throws.
    class SpillReservation
    {
    public:
        SpillReservation() = default;
        SpillReservation(const SpillReservation &) = delete;
        SpillReservation & operator=(const SpillReservation &) = delete;
        ~SpillReservation() { release(); }

        /// Waits for writers to release enough budget; gives up only when the query is
        /// cancelled. A request larger than the whole budget is granted when it is alone, so
        /// one oversized table cannot deadlock the valve.
        bool reserveOrWait(AdaptiveAggregationSession & session_, size_t bytes_)
        {
            std::unique_lock lock(session_.detached_spill_mutex);
            session_.detached_spill_cv.wait(
                lock, [&] { return fits(session_, bytes_) || session_.cancelled.load(std::memory_order_relaxed); });
            if (session_.cancelled.load(std::memory_order_relaxed) || !fits(session_, bytes_))
                return false;
            grab(session_, bytes_);
            return true;
        }

        /// Corrects the reservation to the built table's real footprint.
        void resize(size_t bytes_)
        {
            bool released_some = false;
            {
                std::lock_guard lock(session->detached_spill_mutex);
                if (bytes_ >= bytes)
                    session->estimated_detached_spill_bytes += bytes_ - bytes;
                else
                {
                    session->estimated_detached_spill_bytes -= bytes - bytes_;
                    released_some = true;
                }
                bytes = bytes_;
            }
            if (released_some)
                session->detached_spill_cv.notify_all();
        }

        void release()
        {
            if (!session)
                return;
            {
                std::lock_guard lock(session->detached_spill_mutex);
                session->estimated_detached_spill_bytes -= bytes;
            }
            session->detached_spill_cv.notify_all();
            session = nullptr;
            bytes = 0;
        }

    private:
        static bool fits(const AdaptiveAggregationSession & session_, size_t bytes_)
        {
            if (session_.estimated_detached_spill_bytes == 0)
                return true;
            return session_.estimated_detached_spill_bytes < adaptive_pressure_detached_bytes_budget
                && bytes_ <= adaptive_pressure_detached_bytes_budget - session_.estimated_detached_spill_bytes;
        }

        void grab(AdaptiveAggregationSession & session_, size_t bytes_)
        {
            session = &session_;
            bytes = bytes_;
            session_.estimated_detached_spill_bytes += bytes_;
        }

        AdaptiveAggregationSession * session = nullptr;
        size_t bytes = 0;
    };
    /// Set when the query is cancelled (see `cancel`). The pressure sweeps check it between
    /// chunks and buckets, so a dying query does not wait out a full sweep - which can include
    /// spilling gigabytes to disk - before it stops.
    std::atomic<bool> cancelled{false};

    /// Stops the sweeps at their next check and wakes any producer waiting for spill budget;
    /// the fence keeps the store from racing past a waiter that already checked the flag.
    void cancel()
    {
        cancelled.store(true, std::memory_order_relaxed);
        {
            std::lock_guard lock(detached_spill_mutex);
        }
        detached_spill_cv.notify_all();
    }
    std::once_flag init_flag;
    std::atomic<bool> initialized{false};

    /// The thaw sampler (see the tuning constants in `Aggregator.cpp`). At publish the threads
    /// fold a sparse sample of their staged record hashes in here; repeats of a key collapse
    /// onto one entry across all threads, so sampled records per distinct sampled hash estimates
    /// the repeat factor of the staged stream as a whole, independently of how a key's
    /// occurrences spread over the threads.
    std::mutex thaw_sample_mutex;
    HashSet<UInt64> distinct_sampled_hashes;
    size_t thaw_sampled_records = 0;
    size_t staged_records = 0;
    /// Set once the staged stream proves repeat-dominated; every thread then thaws its local
    /// table at the next block and returns to the baseline path for good.
    std::atomic<bool> thaw_all{false};
};

using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
/// phase-owned counters, and the thread's staged-chunk builder (created by
/// `Aggregator::createAdaptiveProducer`, which knows the aggregate-argument positions the
/// builder gathers).
struct AdaptiveAggregationProducer
{
    AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_, StagedChunkBuilder staging_)
        : session(std::move(shared_))
        , staging(std::move(staging_))
    {
    }

    /// The thread starts learning: the local table inserts as usual while the freeze rule
    /// watches its growth. Rows consumed here feed the give-up rule (see `executeOnBlock`).
    struct LearningState
    {
        size_t rows_seen = 0;
    };

    /// The adaptive phase proper: the local table only updates the keys it already holds
    /// and misses are staged for the shared drain. Carries the post-freeze hit-rate
    /// sampling: when the frozen table turns out to hold almost none of the stream's keys
    /// (a uniform high-cardinality distribution), probing it is pure overhead on every row;
    /// after the sample window the kernel switches to staging every row without the lookup.
    struct FrozenState
    {
        size_t sampled_rows = 0;
        size_t sampled_hits = 0;
        bool bypass_local_probe = false;
    };

    /// Terminal: the thread aggregates exactly as with the feature off, keeping only the
    /// reason it stood down.
    struct BaselineState
    {
        enum class Reason
        {
            /// The give-up rule: the table stayed far below the freeze threshold across
            /// many times that many rows, so the stream is repeat-dominated locally.
            TooFewDistinctKeys,
            /// The global thaw: the session-wide staged-key sample proved the whole stream
            /// repeat-dominated (see `stageRecordedMisses`).
            RepeatedStagedKeys,
        };
        Reason reason;
    };

    using Phase = std::variant<LearningState, FrozenState, BaselineState>;
    Phase phase = LearningState{};

    bool isLearning() const { return std::holds_alternative<LearningState>(phase); }
    bool isFrozen() const { return std::holds_alternative<FrozenState>(phase); }
    bool isBaseline() const { return std::holds_alternative<BaselineState>(phase); }

    void freeze() { phase = FrozenState{}; }
    void standDown(BaselineState::Reason reason) { phase = BaselineState{.reason = reason}; }

    AdaptiveAggregationSessionPtr session;

    StagedChunkBuilder staging;
};

/// The production-time destination of sealed staged chunks: finishes the chunk and publishes
/// it to the session's per-bucket backlogs (see `Aggregator::publishStagedChunk`).
struct StagedChunkBacklogSink final : IStagedChunkSink
{
    StagedChunkBacklogSink(const Aggregator & aggregator_, AdaptiveAggregationSession & session_)
        : aggregator(aggregator_), session(session_)
    {
    }

    void consume(MutableStagedChunkPtr chunk) override;

private:
    const Aggregator & aggregator;
    AdaptiveAggregationSession & session;
};

struct StagedChunkPreparation
{
    Columns materialized_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};

}
