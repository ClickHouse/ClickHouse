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
#include <base/PackedStringRef.h>

namespace DB
{

class Arena;

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
/// carry a pointer into the source block. Conversion copies key bytes and gathers arguments
/// into owned columns so the source block can be released. Both string kinds stage raw characters,
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
    /// same bucket-grouped order, so a bucket's slice is a contiguous row range. Only the
    /// aggregate-argument positions are filled, kept at their original indexes so that the
    /// instruction preparation can index the vector. Conversion gathers the rows before
    /// materializing wrapped arguments, so the staged columns are always dense.
    struct AggregatePayload
    {
        Columns argument_columns;

        /// Instructions over `argument_columns`, built by `prepareStagedChunk` after coalescing
        /// and splitting. Preparation is owned by this chunk and stays immutable during admission
        /// and draining, so bucket workers can read it concurrently.
        std::unique_ptr<const StagedChunkPreparation> prepared;

        AggregatePayload();
        AggregatePayload(AggregatePayload &&) noexcept;
        AggregatePayload & operator=(AggregatePayload &&) noexcept;
        ~AggregatePayload();
    };

    StagedKeys keys;
    std::variant<CountPayload, AggregatePayload> payload;

    bool countsOnly() const { return std::holds_alternative<CountPayload>(payload); }

    /// Returns logical key and payload bytes, excluding allocation headroom and prepared instructions.
    size_t byteSize() const;

    /// Returns allocated key and payload buffer bytes, excluding prepared instructions.
    size_t allocatedBytes() const;

    /// Copies a record range and rebases its bucket and key offsets. Aggregate instructions
    /// must be prepared for the returned columns before admission.
    MutableStagedChunkPtr cut(size_t start, size_t length) const;

    /// Returns whether key ranges and payload lengths describe the same records.
    bool isWellFormed() const;
};

/// Records frozen-table misses and converts them into owned, bucket-grouped chunks, then buffers
/// and coalesces small chunks. The producer observes recorded hashes for thaw decisions and prepares
/// returned chunks for admission. The converter owns record layout and copying, not adaptive policy.
class StagedChunkConverter
{
public:
    /// Records a source row and its routing information. Variable-width keys also record their byte size.
    template <typename Key>
    void recordMiss(UInt32 row, UInt64 hash, UInt8 bucket, const Key & key);

    /// Records one count contribution, represented by its first source row and run length.
    template <typename Key>
    void recordCountRun(UInt32 row, UInt64 hash, UInt8 bucket, const Key & key, UInt32 multiplicity);

    /// Tests the hash of the last recorded count run. The caller must also compare the keys.
    bool lastCountRunHasHash(UInt64 hash) const
    {
        return !miss_hashes.empty() && miss_hashes.back() == hash;
    }

    /// Extends the count run after the caller has established key equality.
    void extendLastCountRun()
    {
        chassert(!miss_multiplicities.empty());
        ++miss_multiplicities.back();
    }

    /// Exposes the pre-deduplication hashes for the producer's thaw sample, until `clearMisses`.
    std::span<const UInt64> getRecordedHashes() const { return {miss_hashes.data(), miss_hashes.size()}; }

    /// Returns the key bytes represented by those hashes, before chunk deduplication.
    template <typename Key>
    size_t getRecordedKeyBytes() const;

    /// Builds a candidate without clearing misses, which the producer still needs for thaw sampling.
    template <typename SharedKey, typename State>
    MutableStagedChunkPtr build(
        const Columns & columns,
        const ColumnNumbersList & aggregates_positions,
        State & local_find_state,
        Arena & scratch_pool,
        bool counts_only,
        std::optional<UInt32> key_row_override);

    /// Returns a candidate of at least half the byte target directly, leaving buffered chunks pending.
    /// Smaller candidates are buffered and coalesced when their combined bytes reach the target;
    /// returns null while they remain buffered.
    MutableStagedChunkPtr stage(MutableStagedChunkPtr chunk);

    /// Returns the coalesced pending candidates, or null when there are none.
    MutableStagedChunkPtr flush();

    /// Clears all recorded fields together after observation, retaining their capacity for the next block.
    void clearMisses();

private:
    /// The current block's misses, one entry per delayed record, in staging order.
    PaddedPODArray<UInt32> miss_source_rows;
    PaddedPODArray<UInt64> miss_hashes;
    PaddedPODArray<UInt8> miss_buckets;
    PaddedPODArray<UInt64> miss_key_sizes;
    PaddedPODArray<UInt32> miss_multiplicities;

    template <typename SharedKey, typename State>
    void buildCountChunk(
        StagedChunk & block, State & local_find_state, Arena & scratch_pool, std::optional<UInt32> key_row_override);

    template <typename SharedKey, typename State>
    void buildAggregateChunk(
        StagedChunk & block, const Columns & columns, const ColumnNumbersList & aggregates_positions,
        State & local_find_state, Arena & scratch_pool, std::optional<UInt32> key_row_override);

    static void coalesceCountChunksWithDeduplication(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk);

    /// Scratch for count-record grouping: the records' staging indexes in group
    /// order (the hashes stay in `miss_hashes`, so the entries are four bytes, not sixteen).
    std::vector<UInt32> grouped_index_scratch;
    std::vector<UInt32> group_offsets_scratch;
    std::vector<UInt32> group_cursor_scratch;

    /// Tracks deduplication when building and coalescing chunks separately: a stream can have
    /// distinct keys within each block but repeated keys across blocks. Bypassed duplicates
    /// still merge during draining, so bypassing affects staging cost without changing results.
    struct DedupProductivity
    {
        size_t consecutive_unproductive = 0;
        size_t passes_since_resample = 0;
        bool bypassed = false;

        /// Runs every pass while engaged and periodically while bypassed to detect distribution changes.
        bool shouldDedup()
        {
            if (!bypassed)
                return true;
            if (++passes_since_resample < adaptive_dedup_resample_interval)
                return false;
            passes_since_resample = 0;
            return true;
        }

        /// A productive pass re-enables deduplication. Consecutive passes that remove fewer
        /// than 1/64 of their input records eventually enable bypassing.
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
    };

    DedupProductivity block_dedup;
    DedupProductivity coalescing_dedup;

    /// Small per-block staging batches buffered for coalescing: they are merged into one
    /// bucket-grouped chunk before they reach the backlogs (see `stage`), so the
    /// merge-time drain gets a few large contiguous slices per bucket instead of one tiny
    /// slice per consumed block. Flushed by `flush` when the input ends.
    std::vector<MutableStagedChunkPtr> pending_chunks;
    size_t pending_staged_bytes = 0;
};

}
