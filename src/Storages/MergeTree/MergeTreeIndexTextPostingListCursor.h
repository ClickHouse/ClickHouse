#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <base/defines.h>
#include <base/types.h>

#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListSegment.h>

#include <memory>
#include <span>
#include <vector>

namespace DB
{

struct TokenPostingsInfo;
class TextIndexPostingsCache;
class IColumn;
class MergeTreeReaderStream;

/// Inline capacity for the embedded posting list buffer.
constexpr size_t MAX_EMBEDDED_POSTING_LIST_ROWS = 6;

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
    /// Compressed posting list: state lives in `.pst` and is decoded lazily.
    /// Decoded segments are memoized in `postings_cache_` (keyed by `index_id_` and the segment's byte
    /// offset) and shared across all per-task cursors and queries, avoiding redundant per-task segment
    /// reads/parsing under parallel reads. The cache is always available — the text-index condition
    /// creates a per-query one when the global cache is disabled.
    PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, TextIndexPostingsCache & postings_cache_, const String & index_id_ = {});

    /// Embedded posting list (small inline postings, or a materialized rare-token bitmap).
    /// The cursor owns a copy of `info_` and pre-decodes the postings into `embedded_values`.
    explicit PostingListCursor(const TokenPostingsInfo & info_);

    /// Embedded posting list backed by a pre-flattened, shared, immutable sorted array.
    /// Used for the analyzer-folded postings of eagerly-read tokens: the flattened array is
    /// built once per granule and shared across all per-task cursors, avoiding a per-cursor
    /// Roaring deep copy and `toUint32Array` materialization. Cardinality, density and the
    /// row-id range are derived directly from the (sorted) array, so no `TokenPostingsInfo`
    /// is needed. The cursor only keeps its own read position; the array data is read-only
    /// and safe to share across threads.
    explicit PostingListCursor(std::shared_ptr<const std::vector<UInt32>> shared_values_);

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
    /// Load metadata for `segment_idx`-th segment.
    /// For compressed postings: obtains the decoded segment (payload + packed block index) — from the
    /// shared `TextIndexPostingsCache` if available, otherwise built via `buildPostingSegment` — and
    /// points the segment views at it. Does NOT decode any packed block data yet.
    /// For embedded postings: no-op — `embedded_values` already holds the decoded array.
    void prepareSegment(size_t segment_idx);

    /// Reads and parses one compressed segment from `stream` into an immutable `PostingListSegment`.
    /// Invoked on a cache miss (or directly when no posting cache is available).
    PostingListSegmentPtr buildPostingSegment(size_t segment_idx);

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

    /// On-disk description of the posting list. For compressed postings this is a
    /// non-owning pointer into the granule's token map (which outlives the cursor);
    /// for embedded/rare postings the info is owned via `owned_info` to keep the
    /// materialized bitmap alive.
    MergeTreeReaderStream * stream = nullptr;
    std::shared_ptr<TokenPostingsInfo> owned_info;
    const TokenPostingsInfo * info = nullptr;

    /// Bounded cache used to memoize decoded segments across per-task cursors (and queries, when the
    /// global cache is enabled). Set for compressed cursors via the stream constructor; stays null for
    /// embedded cursors, which never read segments (`prepareSegment` returns early for `is_embedded`).
    TextIndexPostingsCache * postings_cache = nullptr;
    /// Per-part index identifier, mixed into the segment cache key alongside the segment byte offset.
    String index_id;

    size_t total_segments = 0;
    bool is_embedded = false;
    double density_val = 0;

    /// Pre-decoded embedded postings.
    /// Inline buffer for small embedded posting lists; spills to the heap when the
    /// materialized list (e.g. a rare-token roaring bitmap) exceeds the inline capacity.
    absl::InlinedVector<uint32_t, MAX_EMBEDDED_POSTING_LIST_ROWS> embedded_values;

    /// When set (shared-array embedded ctor), the embedded postings are read from this shared,
    /// immutable, sorted array instead of `embedded_values`. Built once per granule and shared
    /// across per-task cursors. Held to keep the buffer alive for the cursor's lifetime;
    /// `decoded_values_ptr` points into it.
    std::shared_ptr<const std::vector<UInt32>> shared_values;

    /// Row-id range [begin, end] covered by an embedded posting list, used for the dense-range
    /// shortcut in `linearOr` / `linearAnd`. Populated by both embedded constructors (from
    /// `info_.ranges`, or from the sorted array's first/last element).
    size_t embedded_range_begin = 0;
    size_t embedded_range_end = 0;

    /// Decoded doc_ids of the current packed block. Used as a scratch buffer when
    /// iterating compressed posting lists; `decoded_values_ptr` is then redirected to
    /// point at this buffer. For embedded posting lists, `decoded_values_ptr` instead
    /// points directly at `embedded_values`, avoiding a copy and supporting embedded
    /// lists larger than BLOCK_SIZE.
    alignas(16) uint32_t decoded_values[BLOCK_SIZE]{};
    const uint32_t * decoded_values_ptr = decoded_values;
    size_t decoded_count = 0;    /// Number of valid entries reachable via `decoded_values_ptr`.
    size_t index = 0;            /// Read position within `decoded_values_ptr`.

    /// Per-segment packed block layout (recomputed in `prepareSegment`).
    size_t block_count = 0;              /// Total packed blocks, including the tail block.
    size_t current_block = 0;            /// Index of the packed block being iterated.
    size_t tail_size = 0;                /// Element count of the tail block (< BLOCK_SIZE), 0 if aligned.
    UInt32 segment_doc_count = 0;        /// Total doc count in the current segment.
    UInt32 last_decoded_doc_id = 0;      /// Last doc_id decoded (delta base for next block).
    UInt32 segment_first_row_id = 0;     /// First row_id of the current segment (for delta base).

    /// Decoded data of the current segment. Owned by the shared `TextIndexPostingsCache` (or by the
    /// cursor itself when no cache is available); held here to keep it alive while the views below
    /// point at it. Surviving cache eviction is intentional — an in-flight cursor stays valid.
    PostingListSegmentPtr current_segment_data;

    /// Non-owning views into `current_segment_data`, refreshed in `prepareSegment`.
    ///   block_last_row_ids / block_offsets — packed block index, enabling O(log N) advance.
    ///   payload_buffer — bulk-loaded segment payload; `decodeBlock` works from memory.
    std::span<const UInt32> block_last_row_ids;
    std::span<const UInt64> block_offsets;
    std::span<const uint8_t> payload_buffer;

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
        /// "Skipped" counters are shared between the OR and AND paths: a region is skipped either
        /// because the segment is fully dense (`segments_skipped_dense`) or because its output is
        /// already resolved — all-ones for OR, all-zeros for AND (`*_skipped_resolved`).
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
