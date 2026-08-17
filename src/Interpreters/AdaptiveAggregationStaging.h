#pragma once

#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

#include <base/PackedStringRef.h>
#include <base/defines.h>
#include <base/memcmpSmall.h>
#include <Columns/IColumn_fwd.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/memcpySmall.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class Arena;
struct StagedChunkPreparation;

/// The staging side of the adaptive aggregation: the frozen kernels record the rows their
/// local table missed, and this module converts one block's recorded misses into a staged
/// chunk - the records grouped by the two-level bucket of their key's hash, with the key
/// bytes and routing hashes staged next to the payload - coalesces small chunks, and hands
/// the sealed result to a sink. The module knows nothing about the aggregator: everything
/// it needs (the aggregate-argument positions and the key bytes, extracted through the
/// hashing state passed into the templated builds) comes in through its interface, so a
/// future consumer that scatters rows by hash the same way (a grace-style join spilling
/// per-bucket slices, for example) can adopt the bucket-grouped representation and the
/// counting-sort mechanics without pulling in the aggregation machinery.

/// A count-record dedup pass (the build's within-block pass or the seal's cross-mini pass)
/// is bypassed after this many consecutive passes whose records almost all survived it - the
/// pass costs a per-record candidate scan and finds nothing on a distinct stream. While
/// bypassed, every `adaptive_dedup_resample_interval`-th pass runs the dedup anyway, so a
/// stream whose distribution turns repetitive gets its dedup back.
constexpr size_t adaptive_dedup_unproductive_passes_to_bypass = 4;
constexpr size_t adaptive_dedup_resample_interval = 64;

/// Staged batches smaller than this are coalesced into one bucket-grouped chunk before they
/// reach the backlogs, so the merge-time drain processes a few large contiguous slices per
/// bucket instead of one tiny slice per consumed block; a batch of at least half the target
/// is enqueued as-is. Also bounds the coalescing buffer per thread.
constexpr size_t adaptive_seal_target_bytes = 4 << 20;

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
    /// occupy `key_bytes[keyByteOffsetAt(i), keyByteOffsetAt(i) + keySizeAt(i))`; bucket b owns
    /// the record range [bucket_offsets[b], bucket_offsets[b + 1]). The key bytes are staged so
    /// that the drain emplaces without constructing a hashing state per (chunk, bucket) slice.
    struct StagedKeys
    {
        PaddedPODArray<UInt64> routing_hashes;
        PaddedPODArray<char> key_bytes;
        /// Byte offsets of the records' keys, populated only for variable-size (string-kind)
        /// keys. A fixed-size-key chunk carries no offsets: every position derives from
        /// `fixed_key_size`, which saves eight bytes per staged record.
        PaddedPODArray<UInt64> key_offsets;
        /// The staged width of a fixed-size key - `sizeof` of the shared method's key type,
        /// the exact width the kernels stage - or zero for variable-size keys.
        UInt64 fixed_key_size = 0;
        std::array<UInt32, ADAPTIVE_AGGREGATION_NUM_BUCKETS + 1> bucket_offsets{};

        size_t size() const { return routing_hashes.size(); }
        size_t recordsForBucket(size_t bucket) const { return bucket_offsets[bucket + 1] - bucket_offsets[bucket]; }
        UInt64 keyByteOffsetAt(size_t i) const { return fixed_key_size ? i * fixed_key_size : key_offsets[i]; }
        size_t keySizeAt(size_t i) const { return fixed_key_size ? fixed_key_size : key_offsets[i + 1] - key_offsets[i]; }
        std::string_view keyBytesAt(size_t i) const { return {key_bytes.data() + keyByteOffsetAt(i), keySizeAt(i)}; }
    };

    /// Value staging (simple-count aggregation only): the record is the key itself plus a run
    /// length - `multiplicities[i]` is how many source rows record i represents (repeats of a
    /// key collapse into one record at staging time).
    struct CountPayload
    {
        PaddedPODArray<UInt32> multiplicities;
    };

    /// Row-reference staging (general aggregates): record i reads its aggregate arguments from
    /// row i of `argument_columns`, which hold the records' values gathered at build time in the
    /// same bucket-grouped order, so a bucket's slice is a contiguous row range. Only the
    /// aggregate-argument positions are filled, kept at their original indexes so that the
    /// instruction preparation can index the vector; sparse arguments are materialized by the
    /// gather, so the staged columns are always dense.
    struct AggregatePayload
    {
        Columns argument_columns;

        /// The aggregate-function instructions over `argument_columns`, built in the chunk's
        /// own stable storage when the chunk is published (see `Aggregator::publishStagedChunk`),
        /// so a published chunk is immutable and the drains read it without coordination.
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
    bool wellFormed() const;
};

/// A published chunk is immutable; only the producer building a chunk holds it mutably.
using StagedChunkPtr = std::shared_ptr<const StagedChunk>;
using MutableStagedChunkPtr = std::shared_ptr<StagedChunk>;

/// The destination of sealed staged chunks. The builder is destination-agnostic: during
/// production the aggregator publishes the chunks to the session's per-bucket backlogs, and
/// a different owner can route them elsewhere (a pipeline port, a join's bucket store)
/// without touching the conversion. Called once per sealed chunk, so the virtual dispatch
/// costs nothing on the per-record scale.
struct IStagedChunkSink
{
    virtual void consume(MutableStagedChunkPtr chunk) = 0;
    virtual ~IStagedChunkSink() = default;
};

/// String-like keys stage their bytes: a packed reference copied as a plain value would
/// carry a pointer into the source block, which dies when the build compacts the arguments
/// and releases it. The staged form of both string kinds is the raw characters, and the
/// drain rebuilds the table's key from them.
template <typename Key>
constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

/// How far past a key's bytes a reader may touch. The overflow-tolerant small copy and
/// compare primitives access up to 15 bytes past the end, which is only legal for bytes
/// living in padded containers (column chars, arenas, the staged arrays); an exact-size
/// allocation forbids them.
enum class ReadablePadding
{
    Exact,
    AtLeast15Bytes,
};

struct KeyBytesRef
{
    std::string_view bytes;
    ReadablePadding padding;
};

/// Compare a staged key (always in a padded container) with candidate bytes of the same
/// size. The overflow-tolerant primitive reads past both ends, so it is gated on the
/// candidate's padding; small keys are where it beats a libc call.
inline bool ALWAYS_INLINE stagedKeyEquals(const char * staged, const KeyBytesRef & key)
{
    if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
        return memequalSmallAllowOverflow15(staged, key.bytes.size(), key.bytes.data(), key.bytes.size());
    return memcmp(staged, key.bytes.data(), key.bytes.size()) == 0;
}

/// Copy candidate bytes into staged (padded) storage, honoring the source's padding. The
/// overflow-tolerant branch also writes up to 15 bytes past the destination, so the callers
/// must append in increasing byte order (the scribble lands in space the next append
/// overwrites); a caller that scatters must use a plain bounded copy instead.
inline void ALWAYS_INLINE copyStagedKeyBytes(char * staged, const KeyBytesRef & key)
{
    if (key.bytes.empty())
        return;
    if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
        memcpySmallAllowReadWriteOverflow15(staged, key.bytes.data(), key.bytes.size());
    else
        memcpy(staged, key.bytes.data(), key.bytes.size());
}

/// The count-record dedup primitive shared by the build and seal walks. A duplicate key
/// can only be one of its group's survivors, the records staged in [group_begin, out) with
/// the same few hash bits (usually zero or one): merge the run lengths instead of staging
/// another copy of the key, with equal hashes of distinct keys split by the byte
/// comparison. Otherwise the record is appended at `out` and the cursors advance.
///
/// The overflow-split policy lives here and only here: a survivor whose multiplicity would
/// exceed 32 bits is skipped, because a later survivor of the same key (from a previous
/// overflow split) may still have capacity, and otherwise the record starts a fresh
/// survivor of the same key.
inline void ALWAYS_INLINE mergeOrAppendStagedCount(
    StagedChunk::StagedKeys & keys,
    PaddedPODArray<UInt32> & multiplicities,
    const UInt64 hash,
    const KeyBytesRef & key,
    const UInt32 multiplicity,
    const size_t group_begin,
    size_t & out,
    UInt64 & byte_pos)
{
    const size_t size = key.bytes.size();
    for (size_t j = group_begin; j < out; ++j)
    {
        if (keys.routing_hashes[j] != hash)
            continue;
        /// A fixed-size-key chunk needs no size comparison: every record is `size` wide.
        if (keys.fixed_key_size)
        {
            if (!stagedKeyEquals(keys.key_bytes.data() + j * keys.fixed_key_size, key))
                continue;
        }
        else
        {
            const UInt64 j_end = (j + 1 == out) ? byte_pos : keys.key_offsets[j + 1];
            if (j_end - keys.key_offsets[j] != size || !stagedKeyEquals(keys.key_bytes.data() + keys.key_offsets[j], key))
                continue;
        }
        if (static_cast<UInt64>(multiplicities[j]) + multiplicity > std::numeric_limits<UInt32>::max())
            continue;
        multiplicities[j] += multiplicity;
        return;
    }

    keys.routing_hashes[out] = hash;
    multiplicities[out] = multiplicity;
    if (!keys.fixed_key_size)
        keys.key_offsets[out] = byte_pos;
    copyStagedKeyBytes(keys.key_bytes.data() + byte_pos, key);
    byte_pos += size;
    ++out;
}

/// Converts one block's recorded misses into a staged chunk, coalesces small chunks, and
/// emits the sealed result to a sink. One builder per producer thread; the kernels append
/// to `misses` while probing and hand the batch over through `stageMisses` at the end of
/// the block. The templated builds extract the key bytes through the hashing state they
/// are given, so the builder itself carries no aggregation method - only the argument
/// positions it gathers and the aggregate count.
class StagedChunkBuilder
{
public:
    StagedChunkBuilder(ColumnNumbersList aggregates_positions_, size_t aggregates_size_, LoggerPtr log_)
        : aggregates_positions(std::move(aggregates_positions_))
        , aggregates_size(aggregates_size_)
        , log(std::move(log_))
    {
    }

    /// The current block's misses, one entry per delayed record, in staging order. Written
    /// directly by the frozen kernels; the arrays are cleared but keep their capacity across
    /// blocks.
    struct RecordedMisses
    {
        PaddedPODArray<UInt32> source_rows;
        PaddedPODArray<UInt64> hashes;
        PaddedPODArray<UInt8> buckets;
        PaddedPODArray<UInt64> key_sizes;
        PaddedPODArray<UInt32> multiplicities;

        void clear()
        {
            source_rows.clear();
            hashes.clear();
            buckets.clear();
            key_sizes.clear();
            multiplicities.clear();
        }
    };

    RecordedMisses misses;

    /// Groups the recorded misses by bucket (counting sort) into one staged chunk and stages
    /// it: a chunk of at least half the seal target goes straight to the sink, a small one is
    /// buffered until enough bytes accumulate for a seal. Key bytes are copied exactly once,
    /// straight from the hashing state's key holder into their bucket position; row-reference
    /// mode additionally gathers the records' aggregate-argument values into dense compacted
    /// columns. Clears `misses`.
    template <typename SharedKey, typename State>
    void stageMisses(
        const Columns & columns,
        size_t num_rows,
        State & local_find_state,
        Arena & scratch_pool,
        bool counts_only,
        std::optional<UInt32> key_row_override,
        IStagedChunkSink & sink);

    /// Seals and emits whatever is buffered; called when the producer's input ends and before
    /// a pressure drain, which must see every staged record.
    void flush(IStagedChunkSink & sink);

private:
    /// Fills a value-staged chunk with the current misses grouped by bucket (and by a few hash
    /// bits within it, so a duplicate can only be one of its group's survivors) and merged:
    /// duplicate keys within the block collapse into one record with a summed run length, so a
    /// repeat-heavy staged stream copies each key's bytes once and the drain emplaces it once.
    template <typename SharedKey, typename State>
    void buildCountChunk(StagedChunk & chunk, State & local_find_state, Arena & scratch_pool, std::optional<UInt32> key_row_override);

    /// The aggregate-payload counterpart of `buildCountChunk`: counting-sorts the staged
    /// misses into bucket-grouped order, stages their key bytes, and gathers the
    /// aggregate-argument columns into the same order (see `StagedChunk::AggregatePayload`).
    template <typename SharedKey, typename State>
    void buildAggregateChunk(
        StagedChunk & chunk,
        const Columns & columns,
        State & local_find_state,
        Arena & scratch_pool,
        std::optional<UInt32> key_row_override);

    /// The seal-or-emit policy for one built chunk (see `stageMisses`).
    void stageBuiltChunk(MutableStagedChunkPtr chunk, size_t estimated_payload_bytes, IStagedChunkSink & sink);

    /// Merges the buffered batches into one bucket-grouped chunk of the same shape (bucket b's
    /// records are the concatenation of the batches' b-slices) and emits it.
    void sealPending(IStagedChunkSink & sink);

    /// The value-staged variant of the seal merge: keys repeating across the batches collapse
    /// into one record with a summed run length while the records are copied into the chunk.
    void sealCountChunkDeduplicated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk);

    /// Productivity tracking of a count-record dedup pass. The build's within-block pass and
    /// the seal's cross-mini pass are tracked separately, because a stream can be distinct
    /// within blocks yet repetitive across them. Bypassing is always correct - duplicate count
    /// records merge at the drain's emplace - so a stale decision costs staged memory until
    /// the next resample, never results.
    struct DedupProductivity
    {
        size_t consecutive_unproductive = 0;
        size_t passes_since_resample = 0;
        bool bypassed = false;

        /// Whether the next pass should dedup: always while engaged, and periodically as a
        /// resample while bypassed, so a distribution change re-engages the dedup.
        bool shouldDedup()
        {
            if (!bypassed)
                return true;
            if (++passes_since_resample < adaptive_dedup_resample_interval)
                return false;
            passes_since_resample = 0;
            return true;
        }

        /// Feeds back a pass that ran the dedup: one productive pass re-engages, enough
        /// consecutive passes that merged almost nothing (less than 1/64 of the records)
        /// bypass.
        void record(size_t input_records, size_t surviving_records)
        {
            if (input_records == 0)
                return;
            if (surviving_records * 64 > input_records * 63)
            {
                if (++consecutive_unproductive >= adaptive_dedup_unproductive_passes_to_bypass)
                    bypassed = true;
            }
            else
            {
                consecutive_unproductive = 0;
                bypassed = false;
            }
        }
    };

    DedupProductivity build_dedup;
    DedupProductivity seal_dedup;

    /// Scratch for the value-staged build grouping: the records' staging indexes in group
    /// order (the hashes stay in `misses.hashes`, so the entries are four bytes, not sixteen).
    std::vector<UInt32> grouped_index_scratch;
    std::vector<UInt32> group_offsets_scratch;
    std::vector<UInt32> group_cursor_scratch;

    /// Small per-block staging batches buffered for coalescing: they are merged into one
    /// bucket-grouped chunk before they reach the sink (see `stageBuiltChunk`), so the
    /// merge-time drain gets a few large contiguous slices per bucket instead of one tiny
    /// slice per consumed block. Flushed by `flush` when the input ends.
    std::vector<MutableStagedChunkPtr> pending_chunks;
    size_t pending_staged_bytes = 0;

    /// The aggregate-argument positions of the aggregation, per aggregate function: the only
    /// aggregation knowledge the builder holds, used to gather the argument columns.
    const ColumnNumbersList aggregates_positions;
    const size_t aggregates_size;

    LoggerPtr log;
};

}
