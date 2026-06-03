#pragma once

#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionTokenInfo.h>
#include <Storages/MergeTree/ProjectionIndex/TurboPForBlockDecodeBuffer.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/PODArray.h>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

struct ProjectionTokenInfo;
class IColumn;
struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;

/// Cursor over a single token's compressed posting list.
///
/// Implements IPostingCursor for use in composable OR/AND trees.
/// Also provides an element-level iterator (seek/next/value) for merge paths.
///
/// Storage layout (two-level hierarchy):
///   Large blocks  — variable-size segments of the posting list, each stored as a
///                   contiguous region in the .pst stream with its own Index Section.
///   Packed blocks — fixed-size TURBOPFOR_BLOCK_SIZE-element groups within a large block,
///                   delta-encoded and compressed with TurboPFor.
class ProjectionPostingListCursor final : public IPostingCursor
{
public:
    ProjectionPostingListCursor(
        LargePostingListReaderStreamPtr owned_stream_,
        const ProjectionTokenInfo & info_,
        LargePostingListReaderStreamPtr idx_stream_ = nullptr,
        LargePostingListReaderStreamPtr pos_stream_ = nullptr,
        DecodedBlockCache * decoded_block_cache_ = nullptr);
    explicit ProjectionPostingListCursor(const ProjectionTokenInfo & info_);

    /// IPostingCursor: scatter-write matching doc_ids into the output buffer.
    void fill(UInt8 * out, size_t row_offset, size_t num_rows) override;

    /// Batch-collect matching doc_ids into a vector (no bitmap scatter).
    /// More efficient than fill()+scan when the result set is sparse.
    void collectDocIds(PaddedPODArray<uint32_t> & out, size_t row_offset, size_t num_rows);

    /// Posting list density: cardinality / (max_doc_id - min_doc_id + 1).
    double density() const override { return density_value; }

    /// Total number of doc_ids in the posting list.
    UInt32 cardinality() const override;

    /// First/last doc_id in the posting list. Returns 0 if empty.
    uint32_t firstDocId() const;
    uint32_t lastDocId() const;

    // ── Element-level iterator (for merge paths and tests) ───────────────

    void next();
    bool valid() const { return is_valid; }

    uint32_t value() const
    {
        if (arithmetic_mode) [[unlikely]]
            return arithmetic_first + static_cast<uint32_t>(index) * arithmetic_step;
        return decoded_values[index];
    }

    void seek(uint32_t target);

private:
    const LargeBlockData & ensureLargeBlock(size_t idx);

    void markLargeBlock(size_t large_block, UInt8 * __restrict out, size_t row_begin, size_t row_end);
    void collectLargeBlock(size_t large_block, PaddedPODArray<uint32_t> & out, size_t row_begin, size_t row_end);

    template <typename ArithmeticVisitor, typename DecodedVisitor>
    void iterateLargeBlock(
        size_t large_block, size_t row_begin, size_t row_end, ArithmeticVisitor && on_arithmetic, DecodedVisitor && on_decoded);

    void resetLargeBlockPosition(size_t target_row);
    bool seekImpl(uint32_t target);
    bool loadPackedBlock(size_t block_idx);

    LargePostingListReaderStream * stream = nullptr;
    LargePostingListReaderStreamPtr owned_stream;
    LargePostingListReaderStreamPtr idx_stream;
    LargePostingListReaderStreamPtr pos_stream; /// non-null when phrase support is active
    const ProjectionTokenInfo & info;

    /// Element-level iterator state.
    /// Points to either decode_buf (self-decoded) or a DecodedBlockCache::Entry (cache hit).
    const uint32_t * decoded_values = nullptr;
    alignas(16) uint32_t decode_buf[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
    size_t decoded_count = 0;
    size_t index = 0;
    size_t current_packed_block = 0;

    /// Lazily-loaded large block data.
    absl::btree_map<size_t, std::shared_ptr<LargeBlockData>> large_blocks;
    size_t cached_large_block_idx = SIZE_MAX;
    const LargeBlockData * cached_large_block_ptr = nullptr;

    /// Sequential read tracking for stream-based large blocks.
    size_t last_sequential_block = SIZE_MAX;
    size_t last_sequential_large_block = SIZE_MAX;

    size_t total_large_blocks = 0;
    size_t current_large_block = 0;

    bool is_valid = true;

    bool arithmetic_mode = false;
    uint32_t arithmetic_step = 0;
    uint32_t arithmetic_first = 0;
    uint32_t arithmetic_count = 0;

    double density_value = 0;

    /// Decode buffer for stream-based .pst reads (lazily initialized).
    std::optional<TurboPForBlockDecodeBuffer> pst_decode_buf;

    /// Optional decoded block cache shared with mark filtering (query-scoped).
    DecodedBlockCache * decoded_cache = nullptr;
};

using ProjectionPostingListCursorPtr = std::shared_ptr<ProjectionPostingListCursor>;
using ProjectionPostingListCursorMap = absl::flat_hash_map<std::string_view, ProjectionPostingListCursorPtr>;

}
