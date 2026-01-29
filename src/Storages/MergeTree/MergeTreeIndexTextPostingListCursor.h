#pragma once
#include <absl/container/flat_hash_map.h>

#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/PODArray.h>

namespace DB
{

struct TokenPostingsInfo;
class WriteBuffer;
class ReadBuffer;
class MergeTreeReaderStream;
class IColumn;

/// A cursor for lazily iterating over a compressed posting list.
///
/// Posting list: a sorted list of row IDs where a token appears.
/// This cursor decodes blocks on-demand, avoiding full decompression upfront.
///
/// Supports two access patterns:
/// 1. Iterator-style: valid() / value() / next() / seek() - for skip-list intersection
/// 2. Linear scan: linearOr() / linearAnd() - for brute-force bitmap operations
///
/// The posting list may span multiple segments. Use addSegment() to
/// register additional segments before iteration.
class PostingListCursor
{
public:
    PostingListCursor(MergeTreeReaderStream * stream_, const TokenPostingsInfo & info_, size_t segment)
        : stream(stream_)
        , info(info_)
    {
        current_values.reserve(BLOCK_SIZE);
        segments.push_back(segment);
        prepare(segment);
    }
    PostingListCursor(const TokenPostingsInfo & info_, size_t segment)
        : PostingListCursor(nullptr, info_, segment)
    {
    }

    /// Register an additional segment to iterate over.
    void addSegment(size_t);

    /// Brute-force: set bits for all row IDs in range [row_offset, row_offset + num_rows).
    void linearOr(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Brute-force: increment counts for all row IDs in range.
    void linearAnd(UInt8 * data, size_t row_offset, size_t num_rows);

    /// Move to next row ID.
    void next();

    /// Returns true if cursor points to a valid row ID.
    bool valid() const { return is_valid; }

    /// Returns current row ID. Requires valid() == true.
    uint32_t value() const { return current_values[index]; }

    /// Advance to first row ID >= target.
    void seek(uint32_t target);

    /// Returns posting list density: count / (max - min + 1).
    /// Used to decide between skip-list vs brute-force algorithm.
    double density() const { return density_val; }

private:
    /// Load and decode header for the given segment.
    void prepare(size_t segment);

    void linearOrImpl(size_t segment, UInt8 *, size_t row_begin, size_t row_end);
    void linearAndImpl(size_t segment, UInt8 *, size_t row_begin, size_t row_end);

    bool seekImpl(uint32_t target);
    uint32_t min() const { return header.first_row_id; }
    uint32_t max() const { return block_row_ends.back(); }

    inline void maybeEraseUnusedSegments(int unused_segment_index)
    {
        chassert(static_cast<size_t>(unused_segment_index) < segments.size());
        if (unused_segment_index >= 0 && segments.size() > 1)
        {
            auto end = segments.begin() + unused_segment_index + 1;
            for (auto it = segments.begin(); it < end; ++it)
                seen_segments.erase(*it);
            segments.erase(segments.begin(), segments.begin() + unused_segment_index + 1);
        }
    }

    MergeTreeReaderStream * stream = nullptr;

    const TokenPostingsInfo & info;
    PostingListCodecBitpackingImpl::Header header;

    /// Decoded row IDs of current block
    std::vector<uint32_t> current_values;
    /// Position within current_values
    size_t index = 0;

    std::string compressed_data;

    size_t block_count = 0;
    size_t current_block = 0;
    size_t tail_size = 0;

    /// Block-level index for efficient seek/skip operations.
    /// These are read from the encoded posting list when posting_list_apply_mode=lazy.
    /// Max row ID of each block (for binary search to skip blocks)
    std::vector<uint32_t> block_row_ends;
    /// Byte offset of each block in compressed_data (for direct block access)
    std::vector<size_t> block_offsets;

    /// Segments (granules) this cursor covers
    std::vector<size_t> segments;
    std::unordered_set<size_t> seen_segments;
    size_t current_segment = std::numeric_limits<size_t>::max();

    bool is_valid = true;
    bool is_embedded = false;
    double density_val = 0;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

/// Compute union (OR) of multiple posting lists using lazy decoding.
///
/// Used for TextSearchMode::Any - a row matches if it contains ANY of the search tokens.
/// Iterates through each posting list and sets corresponding bits in the output column.
///
/// Note: brute_force_apply and density_threshold parameters are unused in union operation
/// since linear scan is always used (no optimization benefit from skip-list for OR).
///
/// @param column         Output column (UInt8), sets bit to 1 for rows matching any token
/// @param postings       Map from token to its posting list cursor
/// @param search_tokens  List of tokens to search (determines iteration order)
/// @param column_offset  Starting position in output column to write results
/// @param row_offset     First row ID in the processing range
/// @param num_rows       Number of rows to process
/// @param brute_force_apply  Unused (kept for API consistency with intersection)
/// @param density_threshold  Unused (kept for API consistency with intersection)
void lazyUnionPostingLists(IColumn & column, const PostingListCursorMap & postings, const std::vector<String> & search_tokens, size_t column_offset, size_t row_offset, size_t num_rows, bool brute_force_apply, float density_threshold);

/// Compute intersection (AND) of multiple posting lists using lazy decoding.
///
/// Used for TextSearchMode::All - a row matches only if it contains ALL search tokens.
/// Employs adaptive algorithm selection based on posting list density:
///
/// Algorithm selection:
///   - Single list (n=1): direct linear scan, equivalent to union
///   - Dense lists (density >= threshold) or brute_force_apply=true:
///     Uses brute-force bitmap counting - each cursor marks its row IDs,
///     then count rows where all cursors have set bits (sequential memory access)
///   - Sparse lists: uses skip-list based leapfrog intersection -
///     cursors advance together, only decode blocks as needed (fewer elements to process)
///
/// The density-based switching optimizes for different access patterns:
///   - Sparse posting lists: skip-list is faster due to fewer elements
///   - Dense posting lists: brute-force is faster due to sequential memory access
///
/// @param column         Output column (UInt8), sets bit to 1 for rows matching all tokens
/// @param postings       Map from token to its posting list cursor
/// @param search_tokens  List of tokens to search (determines which cursors to use)
/// @param column_offset  Starting position in output column to write results
/// @param row_offset     First row ID in the processing range
/// @param num_rows       Number of rows to process
/// @param brute_force_apply  Force brute-force algorithm regardless of density
/// @param density_threshold  Switch to brute-force if average density >= this value
void lazyIntersectPostingLists(IColumn & column, const PostingListCursorMap & postings, const std::vector<String> & search_tokens, size_t column_offset, size_t row_offset, size_t num_rows, bool brute_force_apply, float density_threshold);

}
