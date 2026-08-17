#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include <Common/HashTable/HashSet.h>
#include <Common/SharedMutex.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregationStaging.h>

namespace DB
{

struct AggregatedDataVariants;
using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;

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

    /// The session-global thaw rule. The threads fold a sparse sample of their staged record
    /// hashes in at staging time; repeats of a key collapse onto one entry across all threads,
    /// so sampled records per distinct sampled hash estimates the repeat factor of the staged
    /// stream as a whole, independently of how a key's occurrences spread over the threads.
    /// The verdict weighs the repeats by the records' bytes: storing a key's first record is
    /// the price of storing-it-once (repaid by the merge working on deduplicated keys), and
    /// every further record of the same key is waste the baseline would have absorbed as an
    /// in-place update - so the stream thaws once the wasted bytes per distinct staged key,
    /// (repeat factor - 1) * bytes per record, exceed the bound. The weighting is what
    /// separates the shapes: a near-unique stream never fires regardless of how heavy its
    /// records are (repeat ~ 1), a repetitive stream of tiny count records tolerates high
    /// repeats, and a mildly repetitive stream of wide keys or wide arguments fires early.
    class ThawSampler
    {
    public:
        /// The measured verdict inputs at fire time, for the thaw's log line.
        struct FiredMeasurement
        {
            size_t staged_records;
            size_t staged_bytes;
            double repeat_factor;
            size_t wasted_bytes_per_key;
        };

        /// Folds one staging batch into the sample; `batch_bytes` is the batch's estimated
        /// staged footprint (key bytes + argument bytes + per-record overhead), on the same
        /// pre-deduplication basis as the sampled hashes. Returns the measurement when this
        /// fold fired the thaw; every thread applies a fired thaw at its next block (see
        /// `fired`).
        std::optional<FiredMeasurement> fold(const PaddedPODArray<UInt64> & hashes, size_t batch_bytes);

        bool fired() const { return thaw_all.load(std::memory_order_relaxed); }

        /// The verdict for the hash-table statistics: valid only once enough records were
        /// staged to trust the sampler.
        struct Measurement
        {
            bool measured;
            bool wasteful_staging;
        };
        Measurement measure()
        {
            std::lock_guard lock(mutex);
            return {staged_records >= min_staged_records, fired()};
        }

    private:
        /// The bound on the wasted staged bytes per distinct staged key, calibrated on the
        /// measured shapes with about a 30% margin to the nearest keeper on either side:
        /// firing shapes measure >= ~440 (a 90-byte string key repeating ~3x, a 90-byte
        /// argument on a ~5x-repeating key, and every high-repeat case lands in
        /// the kilobytes), keeping shapes <= ~240 (near-unique streams measure ~0 by
        /// construction, light count records tolerate repeats, and a wide-key count stream
        /// stays under through the smaller record). `min_staged_records` is the trust floor
        /// of the sample; the sparse mask keeps the fold's cost negligible.
        static constexpr UInt64 sample_mask = 0xFF;
        static constexpr size_t min_staged_records = 524'288;
        static constexpr size_t wasted_bytes_per_key_bound = 300;

        std::mutex mutex;
        HashSet<UInt64> distinct_sampled_hashes;
        size_t sampled_records = 0;
        size_t staged_records = 0;
        size_t staged_bytes = 0;
        /// Set once the staged stream proves wasteful; every thread then thaws its
        /// local table at the next block and returns to the baseline path for good.
        std::atomic<bool> thaw_all{false};
    };

    ThawSampler thaw_sampler;
};

using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

}
