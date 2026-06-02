#pragma once

#include <absl/container/flat_hash_map.h>
#include <base/defines.h>
#include <base/types.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListSegment.h>
#include <memory>
#include <vector>

namespace DB
{

struct TokenPostingsInfo;
class TextIndexPostingsCache;
class IColumn;
class MergeTreeReaderStream;

/// Operation type for padding the column with the posting list.
enum class PadOp { Or, And };

/// Lazy cursor over a compressed posting list (sorted row IDs for a token).
///
/// Storage layout (two-level hierarchy):
///   Segments    — variable-size chunks of the posting list, each stored as a
///                 contiguous region in the .pst stream with its own Index Section.
///   Packed blocks — fixed-size BLOCK_SIZE-element groups within a segment,
///                   delta-encoded and compressed with Bitpacking.  The last packed
///                   block in a segment may be shorter (the "tail block").
///
/// Each segment's Index Section (read in `prepareSegment`) stores two parallel arrays:
///   `block_last_row_ids[j]`  — last row_id of packed block j
///   `block_offsets[j]`       — relative byte offset of packed block j within payload
/// These enable O(log N) advance via binary search + random data access.
///
/// Embedded postings (small cardinality tokens) are stored inline as raw values
/// in the dictionary stream and decoded entirely in `prepareSegment`; no .pst stream is used.
///
/// Two access patterns:
///   1. Iterator: `valid` / `value` / `next` / `advance` — for leapfrog intersection.
///   2. Linear scan: `linearOr` / `linearAnd` — for brute-force bitmap operations.
class PostingListCursor
{
public:
    /// Compressed posting list, decoded lazily from `.pst`. With a `postings_cache_`, decoded segments are
    /// memoized (keyed by `index_id_for_cache_` + byte offset) and shared; pass `nullptr` to skip caching.
    PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, TextIndexPostingsCache * postings_cache_ = nullptr, const String & index_id_for_cache_ = {});

    /// Fully-materialized posting list over a pre-flattened, shared, immutable sorted array (analyzer-folded
    /// or already-decoded postings). Cardinality, density and the row-id range derive from the array itself.
    explicit PostingListCursor(FlatPostingsPtr shared_values_);

    /// Flushes batched ProfileEvents counters to the global counters.
    ~PostingListCursor();

    /// Set bits in `data` for all doc_ids in [row_offset, row_offset + num_rows).
    void linearOr(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Increment counters in `data` for all doc_ids in [row_offset, row_offset + num_rows).
    void linearAnd(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Move to the next doc_id.
    void next();

    /// True if cursor points to a valid doc_id.
    bool valid() const { return is_valid; }

    /// Current doc_id. Undefined when `valid` returns false.
    uint32_t value() const { return decoded_values_ptr[index]; }

    /// Advance to the first doc_id >= target.
    void advance(uint32_t target);

    /// Posting list density: cardinality / (max_doc_id - min_doc_id + 1).
    /// Used to choose between leapfrog and brute-force algorithms.
    double density() const { return density_val; }

    /// Total number of doc_ids in the posting list.
    /// Used to sort cursors by selectivity for leapfrog intersection.
    UInt32 cardinality() const;

private:
    /// Point `current_segment` at the `segment_idx`-th segment (from the cache or `buildPostingSegment`)
    /// without decoding block data yet. No-op for shared-array cursors, which already hold the array.
    void prepareSegment(size_t segment_idx);

    /// Reads and parses one compressed segment from `stream` into an immutable `PostingListSegment`.
    /// Invoked on a cache miss (or directly when no posting cache is available).
    PostingListSegment buildPostingSegment(size_t segment_idx);

    /// Advance to the first doc_id >= target within the current segment.
    /// Uses binary search on `block_last_row_ids` for O(log N) access.
    /// Returns false if target exceeds this segment's range.
    bool advanceImpl(uint32_t target);

    /// Decode the packed block at `block_idx` into `decoded_values`.
    void decodeBlock(size_t block_idx);

    /// Linear scan over an embedded (fully materialized) posting list.
    template <PadOp op>
    void linearEmbedded(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Linear scan over a compressed posting list: iterates segments and packed blocks, with
    /// segment- and block-level skips for regions already resolved by `op` (see `canSkipRegion`).
    template <PadOp op>
    void linearSegments(UInt8 * data, size_t row_offset, size_t num_rows);

    /// On-disk description of the posting list: a non-owning pointer into the granule's token map
    /// (which outlives the cursor). Set only for compressed cursors; stays null for the shared-array
    /// cursor, which derives everything it needs from the flattened array and reads no segments.
    MergeTreeReaderStream * stream = nullptr;
    const TokenPostingsInfo * info = nullptr;

    /// Bounded cache used to memoize decoded segments across per-task cursors.
    TextIndexPostingsCache * postings_cache = nullptr;
    /// Per-part index identifier, mixed into the segment cache key alongside the segment byte offset.
    String index_id_for_cache;

    size_t total_segments = 0;
    bool is_embedded = false;
    double density_val = 0;

    /// Set for the shared-array cursor: the postings are read from this shared, immutable, sorted array.
    FlatPostingsPtr shared_values;

    /// Decoded doc_ids of the current packed block. Used as a scratch buffer when
    /// iterating compressed posting lists; `decoded_values_ptr` is then redirected to
    /// point at this buffer. For shared-array cursors, `decoded_values_ptr` instead
    /// points directly into `shared_values`, avoiding a copy and supporting lists
    /// larger than BLOCK_SIZE.
    alignas(16) uint32_t decoded_values[BLOCK_SIZE]{};
    const uint32_t * decoded_values_ptr = decoded_values;
    size_t decoded_count = 0;    /// Number of valid entries reachable via `decoded_values_ptr`.
    size_t index = 0;            /// Read position within `decoded_values_ptr`.

    /// Packed-block iteration state within the current segment.
    size_t current_block = 0;            /// Index of the packed block being iterated.
    UInt32 last_decoded_doc_id = 0;      /// Last doc_id decoded (delta base for next block).

    /// Decoded data of the current segment, read directly wherever the segment layout is needed. Held by
    /// shared_ptr so it stays alive for the cursor's lifetime even after the cache evicts it.
    PostingListSegmentPtr current_segment;

    /// Segment iteration state.
    size_t current_segment_idx = 0;
    bool has_prepared_first_segment = false;
    bool is_valid = true;

    /// ProfileEvents are batched into these local counters and flushed in the destructor
    /// to avoid per-block / per-advance atomic ops on the hot path.
    struct EventsCounters
    {
        size_t blocks_decoded = 0;
        size_t advance_count = 0;
        size_t segments_prepared = 0;
        size_t segments_skipped_dense = 0;
        size_t segments_skipped_resolved = 0;
        size_t blocks_skipped_resolved = 0;
    };

    EventsCounters counters;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

/// Union (OR) of posting lists: set output[row] = 1 if the row appears in ANY posting list.
/// The caller is responsible for preparing the cursor vector (resolving search tokens
/// to cursors and deduplicating if necessary).
void lazyUnionPostingLists(
    IColumn & column,
    const std::vector<PostingListCursorPtr> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows);

/// Intersection (AND) of posting lists: set output[row] = 1 only if the row appears in ALL posting lists.
/// The caller is responsible for preparing the cursor vector (resolving search tokens
/// to cursors and deduplicating if necessary).
///
/// Adaptive algorithm selection based on posting list density:
///   - n == 1:  direct linear scan (degenerate case, same as union).
///   - Dense (min density >= threshold):
///     Brute-force bitmap counting — first cursor sets bits, remaining cursors increment counters,
///     then a final pass keeps only rows where count == n.
///   - Sparse:  leapfrog intersection — cursors sorted by ascending cardinality, the sparsest
///     cursor leads and others advance forward.
void lazyIntersectPostingLists(
    IColumn & column,
    const std::vector<PostingListCursorPtr> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    float density_threshold);

}
