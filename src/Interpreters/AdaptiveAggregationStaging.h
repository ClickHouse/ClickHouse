#pragma once

#include <array>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include <Columns/IColumn_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Common/PODArray.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <Processors/Chunk.h>
#include <base/PackedStringRef.h>

namespace DB
{

class AdaptiveAggregationMissesInfo;

/// Count deduplication runs when building a candidate and when coalescing candidates. Each
/// pass is bypassed after this many consecutive attempts remove almost no records. While
/// bypassed, every `adaptive_dedup_resample_interval`-th attempt samples the stream again so
/// a change in key distribution can re-enable deduplication.
constexpr size_t adaptive_dedup_unproductive_passes_to_bypass = 4;
constexpr size_t adaptive_dedup_resample_interval = 64;
/// Staged batches smaller than this are coalesced into one bucket-grouped chunk before they
/// reach the backlogs, so the merge-time drain processes a few large contiguous slices per
/// bucket instead of one tiny slice per consumed block; a batch of at least half the target
/// is enqueued as-is. Also bounds the coalescing buffer per thread.
constexpr size_t adaptive_coalescing_target_bytes = 4 << 20;

/// String-like keys stage their bytes: a packed reference copied as a plain value would
/// carry a pointer into the source block. Probing records owned key bytes, and conversion gathers
/// arguments into owned columns so the source block can be released. Both string kinds stage raw characters,
/// and the drain rebuilds the table's key from them (the pressure-time drain additionally
/// persists the bytes into its arena; the merge-time drain borrows them).
template <typename Key>
constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

/// The staged records route by the two-level bucket of their key's hash, so the backlogs and
/// the routing structures come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// Owned delayed records grouped by bucket. Conversion builds one candidate per input block;
/// coalescing combines small candidates, and pressure sizing can cut a candidate into pieces.
/// Every chunk uses the same layout, independent of those boundaries.
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
        /// Stores `sizeof` of the shared method's key type for fixed-size keys, matching the
        /// width staged by the kernels. Variable-size keys use zero.
        UInt64 fixed_key_size = 0;
        std::array<UInt32, ADAPTIVE_AGGREGATION_NUM_BUCKETS + 1> bucket_offsets{};

        size_t byteSize() const
        {
            return key_bytes.size() + key_offsets.size() * sizeof(UInt64) + routing_hashes.size() * sizeof(UInt64);
        }
        size_t size() const { return routing_hashes.size(); }
        size_t recordsForBucket(size_t bucket) const { return bucket_offsets[bucket + 1] - bucket_offsets[bucket]; }
        UInt64 keyByteOffsetAt(size_t i) const { return fixed_key_size ? i * fixed_key_size : key_offsets[i]; }
        size_t keySizeAt(size_t i) const { return fixed_key_size ? fixed_key_size : key_offsets[i + 1] - key_offsets[i]; }
        std::string_view keyBytesAt(size_t i) const { return {key_bytes.data() + keyByteOffsetAt(i), keySizeAt(i)}; }
    };

    /// Stores each count record's contribution in `multiplicities[i]`, the number of source
    /// rows represented by record i. Repeated keys can collapse into one staged record.
    struct CountPayload
    {
        PaddedPODArray<UInt32> multiplicities;
    };

    /// General aggregate payload: record i reads its aggregate arguments from
    /// row i of `argument_columns`, which hold the records' values gathered during conversion in the
    /// same bucket-grouped order, so a bucket's slice is a contiguous row range. Distinct arguments
    /// occupy consecutive columns shared by all aggregate instructions that use them.
    /// Conversion gathers rows before materializing wrapped arguments, so staged columns are dense.
    struct AggregatePayload
    {
        Columns argument_columns;

        AggregatePayload();
        AggregatePayload(AggregatePayload &&) noexcept;
        AggregatePayload & operator=(AggregatePayload &&) noexcept;
        ~AggregatePayload();

    private:
        friend class Aggregator;
        /// Publication prepares instructions over final columns before sharing the chunk with bucket workers.
        std::unique_ptr<const StagedChunkPreparation> prepared;
    };

    StagedKeys keys;
    std::variant<CountPayload, AggregatePayload> payload;

    bool countsOnly() const { return std::holds_alternative<CountPayload>(payload); }

    /// Returns logical key and payload bytes, excluding allocation headroom and prepared instructions.
    size_t byteSize() const;

    /// Returns allocated key and payload buffer bytes, excluding prepared instructions.
    size_t allocatedBytes() const;

    /// Copies a record range and rebases its bucket and key offsets. Aggregate instructions
    /// are prepared for the returned columns before backlog publication.
    MutableStagedChunkPtr cut(size_t start, size_t length) const;

    /// Returns whether key ranges and payload lengths describe the same records.
    bool isWellFormed() const;
};

namespace AdaptiveAggregationDetail
{

/// Building and coalescing keep independent productivity histories because repetition can differ
/// within one block and across blocks. Unproductive passes periodically resample the stream.
class DedupProductivity
{
public:
    bool shouldDedup()
    {
        if (!bypassed)
            return true;
        if (++passes_since_resample < adaptive_dedup_resample_interval)
            return false;
        passes_since_resample = 0;
        return true;
    }

    void record(size_t input_records, size_t surviving_records)
    {
        chassert(input_records != 0);
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

private:
    size_t consecutive_unproductive = 0;
    size_t passes_since_resample = 0;
    bool bypassed = false;
};

}

/// Builds one bucket-grouped candidate from a block's recorded misses: a counting sort by bucket,
/// with within-group deduplication of count records, and for aggregate payloads a gather of the
/// forwarded argument columns into dense owned columns. The scratch buffers keep their capacity
/// across blocks.
class StagedChunkConverter
{
public:
    /// `arguments` are the block's forwarded aggregate argument columns, in the transport layout.
    /// `key_column` is the block's forwarded key column when the recording reads its key bytes
    /// from it, and null otherwise.
    Chunk build(
        std::span<const ColumnPtr> arguments, const IColumn * key_column, const AdaptiveAggregationMissesInfo & misses, bool counts_only);

    /// The key bytes of the records the last `build` consumed, before deduplication.
    size_t getRecordedKeyBytes() const { return recorded_key_bytes; }

private:
    template <typename KeyLayout>
    ColumnPtr buildCountColumn(StagedChunk::StagedKeys & keys, const AdaptiveAggregationMissesInfo & misses, const KeyLayout & layout);

    template <typename KeyLayout>
    Columns buildAggregateColumns(
        StagedChunk::StagedKeys & keys,
        std::span<const ColumnPtr> arguments,
        const AdaptiveAggregationMissesInfo & misses,
        const KeyLayout & layout);

    size_t recorded_key_bytes = 0;

    /// Scratch for count-record grouping: the records' indexes in group order (the hashes stay
    /// in the recording, so the entries are four bytes, not sixteen).
    std::vector<UInt32> grouped_index_scratch;
    std::vector<UInt32> group_offsets_scratch;
    std::vector<UInt32> group_cursor_scratch;
    AdaptiveAggregationDetail::DedupProductivity block_dedup;
};

/// Buffers small partitioned chunks and combines corresponding bucket slices. Arguments travel
/// as columns; owned key bytes and bucket offsets remain attached as `StagedKeysInfo`.
class StagedChunkCoalescer
{
public:
    explicit StagedChunkCoalescer(bool counts_only_) : counts_only(counts_only_) {}

    /// Large chunks pass through while smaller chunks accumulate toward the byte target.
    Chunk add(Chunk chunk);
    Chunk flush();
    bool empty() const { return pending_chunks.empty(); }

private:
    bool counts_only;
    Chunks pending_chunks;
    size_t pending_staged_bytes = 0;
    AdaptiveAggregationDetail::DedupProductivity coalescing_dedup;
};

}
