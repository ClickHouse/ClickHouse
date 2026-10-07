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

/// Operation type for padding the column with the posting list.
enum class PadOp { Or, And };

/// Window of rows written by a linear scan (`linearOr` / `linearAnd`): a half-open range [begin, end) of absolute
/// row ids that covers every byte the scan wrote. Empty when the posting list has no rows in the scanned window.
struct PostingsApplyWindow
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

/// Counters of the lazy posting-list operations, accumulated locally and added to the profile events in the
/// destructor, to avoid an atomic `ProfileEvents::increment` per block, advance or granule on the hot path.
struct LazyPostingsStats
{
    LazyPostingsStats() = default;
    LazyPostingsStats(const LazyPostingsStats &) = delete;
    LazyPostingsStats & operator=(const LazyPostingsStats &) = delete;
    ~LazyPostingsStats();

    size_t blocks_decoded = 0;
    size_t advance_count = 0;
    size_t segments_prepared = 0;
    size_t segments_skipped_dense = 0;
    size_t segments_skipped_resolved = 0;
    size_t blocks_skipped_resolved = 0;
    size_t blocks_skipped_dense = 0;
    size_t brute_force_intersections = 0;
    size_t brute_force_early_exits = 0;
    size_t leapfrog_intersections = 0;
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

    /// Sets bits in `data` for all doc_ids in [row_offset, row_offset + num_rows).
    /// Returns the range of rows for which bytes were set.
    PostingsApplyWindow linearOr(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Increments counters in `data` for all doc_ids in [row_offset, row_offset + num_rows).
    /// `num_applied` is the number of posting lists already applied to `data`: a counter equal to it marks a row
    /// present in all of them, and the regions without such rows are skipped.
    /// Returns the range of rows for which counters were incremented.
    PostingsApplyWindow linearAnd(UInt8 * data, size_t row_offset, size_t num_rows, UInt8 num_applied);

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
    /// Returns the range of rows written.
    template <PadOp op>
    PostingsApplyWindow linearEmbedded(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Linear scan over a compressed posting list: iterates segments and packed blocks, with
    /// segment- and block-level skips for regions already resolved by `op` (see `canSkipRegion`).
    /// Returns the range of rows written.
    template <PadOp op>
    PostingsApplyWindow linearSegments(UInt8 * data, size_t row_offset, size_t num_rows, UInt8 num_applied);

    /// Linear scan over the decoded values (`decoded_values_ptr`), resumed from the read position `index`.
    /// Moves `index` past the window and returns the range of rows written.
    /// Inlined: `linearSegments` calls it for every decoded block.
    template <PadOp op>
    ALWAYS_INLINE PostingsApplyWindow linearDecoded(UInt8 * data, size_t row_offset, size_t num_rows);

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
    size_t index = 0;            /// Read position within `decoded_values_ptr`.

    /// Packed-block iteration state within the current segment.
    size_t current_block = 0;            /// Index of the packed block being iterated.
    UInt32 last_decoded_doc_id = 0;      /// Last doc_id decoded (delta base for next block).

    /// Decoded data of the current segment, read directly wherever the segment layout is needed.
    /// Held by shared_ptr so it stays alive for the cursor's lifetime even after the cache evicts it.
    PostingListSegmentPtr current_segment;

    /// Segment iteration state.
    size_t current_segment_idx = 0;
    bool is_valid = true;

    LazyPostingsStats stats;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

/// Sorts the cursors for `lazyUnionPostingLists` by descending density, so the densest cursor fills the output first.
void sortCursorsForUnion(std::vector<PostingListCursor *> & cursors);

/// Sorts the cursors for `lazyIntersectPostingLists` by ascending cardinality,
/// so the sparsest cursor goes first and leads the leapfrog, giving the brute force the earliest exit.
void sortCursorsForIntersection(std::vector<PostingListCursor *> & cursors);

/// Returns `BruteForce` or `Leapfrog` for these cursors.
/// `Auto` picks leapfrog only if it can skip whole blocks of the densest list.
TextIndexPostingsIntersectionAlgorithm chooseIntersectionAlgorithm(const std::vector<PostingListCursor *> & cursors, TextIndexPostingsIntersectionAlgorithm algorithm);

/// Union (OR) of posting lists: set output[row] = 1 if the row appears in ANY posting list.
/// `cursors` must be deduplicated and sorted with `sortCursorsForUnion`.
/// Returns false only if no row of the window is set, so the caller may skip scanning the column.
/// True means that some rows may be set.
bool lazyUnionPostingLists(
    IColumn & column,
    const std::vector<PostingListCursor *> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows);

/// Intersection (AND) of posting lists: set output[row] = 1 only if the row appears in ALL posting lists.
/// `cursors` must be deduplicated and sorted with `sortCursorsForIntersection`.
/// `algorithm` must be the resolved with `chooseIntersectionAlgorithm`.
///
/// The two algorithms, selected by `algorithm`.
///   - Brute-force bitmap counting — the sparsest cursor sets bits,
//      the remaining ones increment counters,
///     then a final pass keeps only the rows where the count is n.
///   - Leapfrog — the sparsest cursor leads, the others are advanced to its doc_id in ascending cardinality,
///     and the first one that overshoots moves the lead forward, so a denser cursor only advances
///     to the doc_ids that all the sparser ones contain.
/// Returns false only if no row of the window is set, so the caller may skip scanning the column.
/// True means that some rows may be set: brute-force intersection may return true when no row survives.
bool lazyIntersectPostingLists(
    IColumn & column,
    const std::vector<PostingListCursor *> & cursors,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows,
    TextIndexPostingsIntersectionAlgorithm algorithm,
    LazyPostingsStats & stats);

}
