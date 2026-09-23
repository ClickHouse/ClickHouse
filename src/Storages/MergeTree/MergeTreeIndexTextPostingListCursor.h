#pragma once

#include <absl/container/flat_hash_map.h>
#include <base/defines.h>
#include <base/types.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Storages/MergeTree/PostingListSegment.h>
#include <Core/SettingsEnums.h>
#include <memory>
#include <vector>

namespace DB
{

struct TokenPostingsInfo;
class TextIndexPostingsCache;
class IColumn;
class MergeTreeReaderStream;

/// Lower bound by galloping search: cheap when the answer is close to `first`, which is the common
/// case for a cursor advancing over dense posting lists, and O(log n) otherwise.
inline const uint32_t * gallopingLowerBound(const uint32_t * first, const uint32_t * last, uint32_t target)
{
    const size_t size = static_cast<size_t>(last - first);
    if (size == 0 || *first >= target)
        return first;

    size_t bound = 1;
    while (bound < size && first[bound] < target)
        bound *= 2;

    /// first[bound / 2] < target, so the answer is in (bound / 2, bound], clamped to the range.
    return std::lower_bound(first + bound / 2 + 1, first + std::min(bound + 1, size), target);
}

/// Operation type for padding the column with the posting list.
enum class PadOp { Or, And };

/// Window of rows written by a linear scan (`linearOr` / `linearAnd`): a half-open range [begin, end) of absolute
/// row ids that covers every byte the scan wrote. Empty when the posting list has no rows in the scanned window.
struct PostingsCursorWindow
{
    size_t begin = 0;
    size_t end = 0;

    bool empty() const { return begin >= end; }

    /// Extend the window to cover [row_begin, row_end). The scans write in ascending order, so a new range
    /// starts past the current end: only `end` moves once the window is not empty.
    void extend(size_t row_begin, size_t row_end)
    {
        chassert(row_begin < row_end);
        chassert(empty() || row_begin >= end);

        if (empty())
            begin = row_begin;

        end = row_end;
    }
};

/// Lazy cursor over a compressed posting list (sorted row IDs for a token).
///
/// Storage layout (two-level hierarchy):
///   Segments    — variable-size chunks of the posting list, each stored as a
///                 contiguous region in the .pst stream with its own Index Section.
///   Packed blocks — fixed-size groups of `IPostingListBlockCodec::BLOCK_SIZE` elements within a segment,
///                   delta-encoded and compressed by the block codec.  The last packed
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
    /// Returns the range of rows written; it is empty when the posting list has no doc_ids in the window.
    PostingsCursorWindow linearOr(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Increment counters in `data` for all doc_ids in [row_offset, row_offset + num_rows).
    /// Returns the range of rows whose counters were incremented; it is empty when the posting list has no doc_ids
    /// in the window. Regions of `data` that are already all-zero are skipped and never reported as written.
    PostingsCursorWindow linearAnd(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Move to the next doc_id. The common case, the next value being in the decoded block,
    /// is resolved inline; block and segment transitions go through `nextSlow`.
    ALWAYS_INLINE void next()
    {
        if (!is_valid)
            return;
        if (++index < decoded_count)
            return;
        nextSlow();
    }

    /// True if cursor points to a valid doc_id.
    bool valid() const { return is_valid; }

    /// Current doc_id. Undefined when `valid` returns false.
    uint32_t value() const { return decoded_values_ptr[index]; }

    /// Advance to the first doc_id >= target. The common case, the target lying within the decoded block,
    /// is resolved inline; block and segment transitions go through `advanceSlow`.
    ALWAYS_INLINE void advance(uint32_t target)
    {
        ++counters.advance_count;
        if (!is_valid)
            return;

        if (index < decoded_count && target <= decoded_values_ptr[decoded_count - 1])
        {
            const auto * it = gallopingLowerBound(decoded_values_ptr + index, decoded_values_ptr + decoded_count, target);
            index = static_cast<size_t>(it - decoded_values_ptr);
            return;
        }

        advanceSlow(target);
    }

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

    /// Slow path of `advance`: the target lies beyond the decoded values of the current block.
    void advanceSlow(uint32_t target);

    /// Slow path of `next`: `index` has run past the decoded values of the current block.
    void nextSlow();

    /// Linear scan over an embedded (fully materialized) posting list.
    /// Returns the range of rows written.
    template <PadOp op>
    PostingsCursorWindow linearEmbedded(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Linear scan over a compressed posting list: iterates segments and packed blocks, with
    /// segment- and block-level skips for regions already resolved by `op` (see `canSkipRegion`).
    /// Returns the range of rows written.
    template <PadOp op>
    PostingsCursorWindow linearSegments(UInt8 * data, size_t row_offset, size_t num_rows);

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
    /// point at this buffer.
    /// For shared-array cursors, `decoded_values_ptr` instead points directly
    /// into `shared_values`, avoiding a copy and supporting arrays larger than a block.
    alignas(16) uint32_t decoded_values[IPostingListBlockCodec::BLOCK_SIZE]{};
    const uint32_t * decoded_values_ptr = decoded_values;

    /// Per-block payload codec for the current segment's codec type; lazily created and reused across all
    /// blocks of this cursor (a posting list is written with a single codec).
    std::unique_ptr<IPostingListBlockCodec> block_codec;

    size_t decoded_count = 0;    /// Number of valid entries reachable via `decoded_values_ptr`.

    /// Read position within `decoded_values_ptr`. The linear scan of an embedded list leaves it at the first
    /// doc_id past the window and resumes its search there: the windows come in ascending order.
    size_t index = 0;

    /// Packed-block iteration state within the current segment.
    /// The linear scan resumes its block search from `current_block`, the last block decoded, for the same reason.
    /// Both resume positions are checked against the window first, so a call out of order only costs a search from the start.
    size_t current_block = 0;            /// Index of the packed block being iterated.
    UInt32 last_decoded_doc_id = 0;      /// Last doc_id decoded (delta base for next block).

    /// Decoded data of the current segment, read directly wherever the segment layout is needed.
    /// Held by shared_ptr so it stays alive for the cursor's lifetime even after the cache evicts it.
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

/// Posting-list doc IDs are 32-bit, so `row_offset > UInt32::max` cannot legitimately occur.
/// Throw a `LOGICAL_ERROR` rather than wrap the offset and corrupt the output column.
void requireRowOffsetRepresentable(size_t row_offset);

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
/// The two algorithms, selected by `algorithm` (`AUTO` compares densities, see `lazyIntersectPostingLists`).
/// In both the cursors are sorted by ascending cardinality, so the sparsest posting list goes first:
///   - Brute-force bitmap counting — the sparsest cursor sets bits, the remaining ones increment counters
///     (skipping regions that are still all-zero), then a final pass keeps only the rows where the count is n.
///     Every cursor after the first scans only the rows the previous cursors wrote, so the blocks outside
///     that range are not even decoded, and the final pass covers that range alone.
///     Stops early once a cursor has no rows in the window, because the intersection is then empty.
///   - Leapfrog — the sparsest cursor leads and the others advance forward, skipping whole blocks.
/// n == 1 is a degenerate case handled by a direct linear scan, same as the union.
void lazyIntersectPostingLists(
    IColumn & column,
    const std::vector<PostingListCursorPtr> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    TextIndexPostingsCursorIntersectionAlgorithm algorithm);

}
