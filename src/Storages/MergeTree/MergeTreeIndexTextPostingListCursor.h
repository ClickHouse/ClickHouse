#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <base/defines.h>
#include <base/types.h>

#include <Storages/MergeTree/BitpackingBlockCodec.h>

#include <memory>
#include <vector>

namespace DB
{

struct TokenPostingsInfo;
class IColumn;
class MergeTreeReaderStream;

/// Inline capacity for the embedded posting list buffer.
constexpr size_t MAX_EMBEDDED_POSTING_LIST_ROWS = 6;

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
    PostingListCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_);

    /// Embedded posting list (small inline postings, or a materialized rare-token bitmap).
    /// The cursor owns a copy of `info_` and pre-decodes the postings into `embedded_values`.
    explicit PostingListCursor(const TokenPostingsInfo & info_);

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
    /// For compressed postings: reads the Index Section from .pst (packed block index),
    /// but does NOT decode any packed block data yet.
    /// For embedded postings: no-op — `embedded_values` already holds the decoded array.
    void prepareSegment(size_t segment_idx);

    /// Advance to the first doc_id >= target within the current segment.
    /// Uses binary search on `block_last_row_ids` for O(log N) access.
    /// Returns false if target exceeds this segment's range.
    bool advanceImpl(uint32_t target);

    /// Decode the packed block at `block_idx` into `decoded_values`.
    void decodeBlock(size_t block_idx);

    /// On-disk description of the posting list. For compressed postings this is a
    /// non-owning pointer into the granule's token map (which outlives the cursor);
    /// for embedded/rare postings the info is owned via `owned_info` to keep the
    /// materialized bitmap alive.
    MergeTreeReaderStream * stream = nullptr;
    std::shared_ptr<TokenPostingsInfo> owned_info;
    const TokenPostingsInfo * info = nullptr;

    size_t total_segments = 0;
    bool is_embedded = false;
    double density_val = 0;

    /// Pre-decoded embedded postings.
    /// Inline buffer for small embedded posting lists; spills to the heap when the
    /// materialized list (e.g. a rare-token roaring bitmap) exceeds the inline capacity.
    absl::InlinedVector<uint32_t, MAX_EMBEDDED_POSTING_LIST_ROWS> embedded_values;

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

    /// Packed block index loaded from Index Section in `prepareSegment`.
    /// Enables O(log N) advance within a segment.
    std::vector<UInt32> block_last_row_ids;
    std::vector<UInt64> block_offsets;

    /// Bulk-loaded segment payload buffer. `prepareSegment` reads the entire
    /// payload [header_end, index_section_start) into this buffer.
    /// `decodeBlock` then works from memory instead of seeking the stream per block.
    std::vector<uint8_t> payload_buffer;

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
        size_t segments_skipped_covered = 0;
        size_t blocks_skipped_covered = 0;
        size_t and_segments_skipped_dense = 0;
        size_t and_segments_skipped_zero = 0;
        size_t and_blocks_skipped_zero = 0;
    };

    EventsCounters counters;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

/// Union (OR) of posting lists: set output[row] = 1 if the row appears in ANY posting list.
void lazyUnionPostingLists(
    IColumn & column,
    const PostingListCursorMap & postings,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows);

/// Intersection (AND) of posting lists: set output[row] = 1 only if the row appears in ALL posting lists.
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
    const PostingListCursorMap & postings,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    float density_threshold);

}
