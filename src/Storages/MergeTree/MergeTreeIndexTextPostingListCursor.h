#pragma once

#include <absl/container/flat_hash_map.h>
#include <base/defines.h>
#include <base/types.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Storages/MergeTree/PostingListSegment.h>
#include <Storages/MergeTree/BM25Kernel.h>
#include <Common/VectorWithMemoryTracking.h>
#include <memory>
#include <utility>
#include <vector>

namespace DB
{

struct TokenPostingsInfo;
class TextIndexPostingsCache;
class IColumn;
class MergeTreeReaderStream;
struct ScoringStats;
struct ScoringPostings;

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
    explicit PostingListCursor(PaddedPODArrayPtr shared_values_);

    /// Flushes batched ProfileEvents counters to the global counters.
    /// Virtual so `PostingListScoringCursor` can extend the cursor polymorphically.
    virtual ~PostingListCursor();

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

protected:
    /// Decodes the packed block. The base implementation decodes the doc-id deltas only and
    /// requires them to consume the whole Index Section span, unless the token carries term
    /// frequencies (then a sub-payload the base cursor does not read follows the deltas).
    virtual void decodeBlock(size_t block_idx);

    /// Point `current_segment` at the `segment_idx`-th segment (from the cache or `buildPostingSegment`)
    /// without decoding block data yet. No-op for shared-array cursors, which already hold the array.
    void prepareSegment(size_t segment_idx);

    /// Reads and parses one compressed segment from `stream` into an immutable `PostingListSegment`.
    /// Invoked on a cache miss (or directly when no posting cache is available).
    PostingListSegment buildPostingSegment(size_t segment_idx);

    /// Decodes postings from the packed block into `decoded_values`.
    /// Returns the number of consumed bytes.
    size_t decodeBlockPostings(size_t block_idx);

    /// Advance to the first doc_id >= target within the current segment.
    /// Uses binary search on `block_last_row_ids` for O(log N) access.
    /// Returns false if target exceeds this segment's range.
    bool advanceImpl(uint32_t target);

    /// Linear scan over an embedded (fully materialized) posting list.
    template <PadOp op>
    void linearEmbedded(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Linear scan over a compressed posting list: iterates segments and packed blocks, with
    /// segment- and block-level skips for regions already resolved by `op` (see `canSkipRegion`).
    template <PadOp op>
    void linearSegments(UInt8 * data, size_t row_offset, size_t num_rows);

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
    PaddedPODArrayPtr shared_values;

    /// Decoded doc_ids of the current packed block. Used as a scratch buffer when
    /// iterating compressed posting lists; `decoded_values_ptr` is then redirected to
    /// point at this buffer.
    /// For shared-array cursors, `decoded_values_ptr` instead points directly
    /// into `shared_values`, avoiding a copy and supporting arrays larger than BLOCK_SIZE.
    alignas(16) uint32_t decoded_values[BLOCK_SIZE]{};
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

/// Per-part cursor over doc-lengths norms of BM25 scoring.
/// Lazily reads segmented .dl stream of the text index.
class DocLengthsCursor
{
public:
    DocLengthsCursor(std::unique_ptr<MergeTreeReaderStream> stream_, const ScoringStats & scoring_stats);

    explicit DocLengthsCursor(PaddedPODArray<UInt8> bytes_);
    ~DocLengthsCursor();

    UInt32 numDocs() const { return num_docs; }

    /// Make the doc ids [row_offset, row_offset + num_rows) resident.
    /// Reads the missing covering `.dl` segments in one pass if needed.
    void ensureRange(size_t row_offset, size_t num_rows);

    /// Returns the `SmallFloat` doc-length byte for the given doc id.
    /// Precondition: `doc_id` lies within the range made resident by the last `ensureRange`.
    UInt8 getByte(UInt32 doc_id) const;

private:
    /// One decompressed, resident segment of the `.dl` stream.
    struct DocLengthsSegment
    {
        UInt64 index = 0;
        PaddedPODArray<UInt8> bytes;
    };

    void updateCachedSegment(UInt32 doc_id) const;

    std::unique_ptr<MergeTreeReaderStream> stream;
    UInt32 num_docs;
    UInt64 segment_size;
    VectorWithMemoryTracking<UInt64> segment_offsets;

    /// Resident segments: one contiguous ascending run covering the last `ensureRange` request.
    /// The in-memory cursor is one fully resident segment for its whole lifetime.
    UInt64 first_resident_segment = 0;
    std::vector<DocLengthsSegment> resident_segments;

    /// Doc-id bounds and data of the segment holding the last `getByte` request.
    mutable UInt32 cached_segment_begin = 0;
    mutable UInt32 cached_segment_end = 0;
    mutable const UInt8 * cached_segment_bytes = nullptr;
};

using DocLengthsCursorPtr = std::shared_ptr<DocLengthsCursor>;

/// Scoring extension of `PostingListCursor`: also decodes per-block term frequencies and exposes
/// the per-block / per-segment block-max upper-bound (UB) inputs for BM25 pruning (WAND / MaxScore).
class PostingListScoringCursor : public PostingListCursor
{
public:
    /// Streaming cursor for compressed multi-block tokens.
    PostingListScoringCursor(
        MergeTreeReaderStream & stream_,
        const TokenPostingsInfo & info_,
        const DocLengthsCursor * doc_lengths_,
        TextIndexPostingsCache * postings_cache_ = nullptr,
        const String & index_id_for_cache_ = {});

    /// Embedded cursor over already-decoded flat postings.
    PostingListScoringCursor(std::shared_ptr<const ScoringPostings> scoring_postings_, const DocLengthsCursor * doc_lengths_);

    /// Exact term frequency of the current row.
    UInt32 termFrequency() const;

    /// `SmallFloat` doc-length byte of the current row.
    UInt8 documentLengthByte() const;

    /// Index of the current packed block (always 0 for the embedded cursor).
    size_t currentBlockIndex() const { return is_embedded ? 0 : current_block; }

    /// Point `current_block` at the block containing `doc_id` without decoding its body.
    void seekBlock(uint32_t doc_id);

    /// Per-block block-max UB inputs of the current segment.
    UInt8 minDocumentLengthByte(size_t block_idx) const;
    UInt8 maxTermFrequencyMinusOne(size_t block_idx) const;

    /// Per-segment block-max UB inputs of the current segment.
    UInt8 segmentMinDocumentLengthByte() const;
    UInt8 segmentMaxTermFrequencyMinusOne() const;

    /// Block-max score upper bound of the current block under weight `w`.
    Float32 blockMaxScore(const BM25Weight & w) const;

    /// Block-max score upper bound over the whole current segment under weight `w`.
    Float32 segmentMaxScore(const BM25Weight & w) const;

protected:
    /// Decodes postings from the packed block into `decoded_values` and term frequencies into `decoded_tfs`.
    void decodeBlock(size_t block_idx) override;

private:
    /// Per-granule `SmallFloat` doc-length cursor, queried by the granule-local row id.
    const DocLengthsCursor * doc_lengths = nullptr;

    /// Term frequencies of the current packed block, parallel to `decoded_values`.
    alignas(16) UInt32 decoded_tfs[BLOCK_SIZE]{};

    /// Embedded-only: the flat postings (sorted row ids with their term frequencies) the cursor iterates.
    std::shared_ptr<const ScoringPostings> embedded_scoring_postings;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

/// A scoring cursor of one scoring token with its BM25 weight and cardinality.
struct ScoreCursor
{
    std::shared_ptr<PostingListScoringCursor> cursor;
    const BM25Weight * weight = nullptr;
    UInt32 cardinality = 0;
};

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

/// Union scorer: per-token union walk over `cursors`, adds each token's BM25 contribution
/// at its hit rows of the window [row_offset, row_offset + num_rows) into `data`.
/// Correct under arbitrary predicate composition (rows may match any subset of the tokens).
void scoreCursorsUnion(
    Float32 * data,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows);

/// Intersection scorer: joint leapfrog over all `cursors`, sums every token's BM25 contribution
/// at each intersection row of the window [row_offset, row_offset + num_rows) into `data`.
/// Valid only under the global `All` search mode.
void scoreCursorsIntersection(
    Float32 * data,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows);

}
