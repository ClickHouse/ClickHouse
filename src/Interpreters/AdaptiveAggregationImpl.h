#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>

#include <Columns/IColumn_fwd.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SharedMutex.h>
#include <Common/PODArray.h>
#include <Interpreters/AdaptiveAggregation.h>
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

/// Staged batches smaller than this are coalesced into one bucket-grouped chunk before they
/// reach the backlogs, so the merge-time drain processes a few large contiguous slices per
/// bucket instead of one tiny slice per consumed block; a batch of at least half the target
/// is enqueued as-is. Also bounds the coalescing buffer per thread.
constexpr size_t adaptive_seal_target_bytes = 4 << 20;
/// A pressure sweep spills the routing table mid-drain only once it holds at least this many
/// keys, so the spilled parts stay reasonably sized instead of one tiny file per chunk.
constexpr size_t adaptive_pressure_spill_min_keys = 1'000'000;

/// The staged records route by the two-level bucket of their key's hash, so the backlogs and
/// the routing structures come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// All delayed records of one consumed block, grouped by bucket. One record batch per
/// consumed block, rather than one per (block, bucket); a thread's small batches are
/// further coalesced into one larger chunk of the same shape before they reach the
/// backlogs.
struct StagedChunk
{
    /// The bucket-grouped key side of a staged chunk, shared by both payload modes: record i's
    /// routing hash (reused by the drain's emplace) is `routing_hashes[i]` and its key bytes
    /// occupy `key_bytes[key_offsets[i], key_offsets[i + 1])`; bucket b owns the record range
    /// [bucket_offsets[b], bucket_offsets[b + 1]). The key bytes are staged so that the drain
    /// emplaces without constructing a hashing state per (chunk, bucket) slice.
    struct StagedKeys
    {
        PaddedPODArray<UInt64> routing_hashes;
        PaddedPODArray<char> key_bytes;
        PaddedPODArray<UInt64> key_offsets;
        std::array<UInt32, ADAPTIVE_AGGREGATION_NUM_BUCKETS + 1> bucket_offsets{};

        size_t size() const { return routing_hashes.size(); }
        size_t recordsForBucket(size_t bucket) const { return bucket_offsets[bucket + 1] - bucket_offsets[bucket]; }
        std::string_view keyBytesAt(size_t i) const
        {
            return {key_bytes.data() + key_offsets[i], key_offsets[i + 1] - key_offsets[i]};
        }
    };

    /// Value staging (simple-count aggregation only): the record is the key itself plus a run
    /// length - `multiplicities[i]` is how many source rows record i represents (repeats of a
    /// key collapse into one record at staging time).
    struct CountPayload
    {
        PaddedPODArray<UInt32> multiplicities;
    };

    /// Row-reference staging (general aggregates): record i reads its aggregate arguments from
    /// row i of `argument_columns`, which hold the records' values gathered at publish in the
    /// same bucket-grouped order, so a bucket's slice is a contiguous row range. Only the
    /// aggregate-argument positions are filled, kept at their original indexes so that the
    /// instruction preparation can index the vector; sparse arguments are materialized by the
    /// gather, so the staged columns are always dense.
    struct AggregatePayload
    {
        Columns argument_columns;

        /// The aggregate-function instructions over `argument_columns`, built in the chunk's
        /// own stable storage when the chunk is published (see `publishStagedChunk`), so a
        /// published chunk is immutable and the drains read it without coordination.
        std::unique_ptr<const StagedChunkPreparation> prepared;

        AggregatePayload();
        AggregatePayload(AggregatePayload &&) noexcept;
        AggregatePayload & operator=(AggregatePayload &&) noexcept;
        ~AggregatePayload();
    };

    StagedKeys keys;
    std::variant<CountPayload, AggregatePayload> payload;

    bool countsOnly() const { return std::holds_alternative<CountPayload>(payload); }

    /// Debug-only structural invariants, checked at publication.
    bool wellFormed() const
    {
        const size_t records = keys.size();
        if (keys.key_offsets.size() != records + 1 || keys.bucket_offsets.back() != records)
            return false;
        for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
            if (keys.bucket_offsets[b] > keys.bucket_offsets[b + 1])
                return false;
        for (size_t i = 0; i < records; ++i)
            if (keys.key_offsets[i] > keys.key_offsets[i + 1])
                return false;
        if (const auto * counts = std::get_if<CountPayload>(&payload))
            return counts->multiplicities.size() == records;
        return true;
    }
};

/// A published chunk is immutable; only the producer building a chunk holds it mutably.
using StagedChunkPtr = std::shared_ptr<const StagedChunk>;
using MutableStagedChunkPtr = std::shared_ptr<StagedChunk>;

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
    /// records into it early (see `pressureDrainStagedBlocks`); it joins the merge set when it
    /// holds data.
    AggregatedDataVariantsPtr early_drain_variants;

    /// Serializes pressure sweeps: one sweeper at a time sheds memory, and a single sweeper
    /// needs no per-bucket coordination; merge-time drains run after the finish barrier and
    /// need none either. Producers over the trigger block on it deliberately - pausing
    /// production is the backpressure that lets the sweep win.
    std::mutex pressure_sweep_mutex;
    /// Whether any early drain moved records into `early_drain_variants`: the finish path then
    /// includes it in the merge set.
    std::atomic<bool> early_drain_started{false};
    /// Set when the query is cancelled. The pressure sweeps check it between chunks and
    /// buckets, so a dying query does not wait out a full sweep - which can include spilling
    /// gigabytes to disk - before it stops.
    std::atomic<bool> cancelled{false};
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
/// phase-owned counters, per-block staging for the missed rows (the arrays are cleared but
/// keep their capacity across blocks), and the buffered chunks awaiting coalescing.
struct AdaptiveAggregationProducer
{
    explicit AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_) : session(std::move(shared_)) { }

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
            /// repeat-dominated (see `publishDelayedRecords`).
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

    /// The current block's misses, one entry per delayed record, in staging order.
    PaddedPODArray<UInt32> miss_source_rows;
    PaddedPODArray<UInt64> miss_hashes;
    PaddedPODArray<UInt8> miss_buckets;
    PaddedPODArray<UInt64> miss_key_sizes;
    PaddedPODArray<UInt32> miss_multiplicities;

    /// Scratch for the value-staged publish grouping (see `buildDeduplicatedCountChunk`).
    std::vector<std::pair<UInt64, UInt32>> sort_pairs_scratch;
    std::vector<UInt32> group_offsets_scratch;
    std::vector<UInt32> group_cursor_scratch;

    /// Small per-block staging batches buffered for coalescing: they are merged into one
    /// bucket-grouped chunk before they reach the backlogs (see `stageChunk`), so the
    /// merge-time drain gets a few large contiguous slices per bucket instead of one tiny
    /// slice per consumed block. Flushed by `flushPendingChunks` when the input ends.
    std::vector<MutableStagedChunkPtr> pending_chunks;
    size_t pending_staged_bytes = 0;
};

struct StagedChunkPreparation
{
    Columns materialized_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};

}
