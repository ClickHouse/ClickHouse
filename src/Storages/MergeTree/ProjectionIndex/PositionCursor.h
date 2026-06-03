#pragma once

#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionTokenInfo.h>
#include <Storages/MergeTree/ProjectionIndex/TurboPForBlockDecodeBuffer.h>
#include <base/types.h>

namespace DB
{

struct ProjectionTokenInfo;
struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;

/// Per-doc position cursor for phrase queries.
///
/// Decodes positions on-demand for individual doc_ids and
/// yields them one at a time via nextPosition().
/// Memory is O(256) fixed per token instead of O(sum_freq).
///
/// Usage:
///   UInt32 freq = cursor->seekDoc(doc_id);
///   for (UInt32 i = 0; i < freq; ++i)
///       UInt32 pos = cursor->nextPosition();
class PositionCursor
{
public:
    PositionCursor(
        LargePostingListReaderStreamPtr pst_stream_,
        LargePostingListReaderStreamPtr pos_stream_,
        LargePostingListReaderStreamPtr idx_stream_,
        const ProjectionTokenInfo & info_);

    /// Prepare to iterate positions for the given doc_id.
    /// Returns the freq (number of positions) for this doc, or 0 if not found.
    UInt32 seekDoc(UInt32 doc_id);

    /// Return the next absolute position for the current doc.
    /// Must be called at most freq times after seekDoc.
    /// Returns NO_MORE_POSITIONS when exhausted.
    UInt32 nextPosition();

    static constexpr UInt32 NO_MORE_POSITIONS = UINT32_MAX;

private:
    /// Find which large block and packed block contain doc_id.
    /// Returns false if doc_id is not in any block.
    bool locateDoc(UInt32 doc_id, size_t & out_lb_idx, size_t & out_pb_idx);

    /// Ensure the packed block (doc_deltas + freqs) at (lb_idx, pb_idx) is decoded.
    void ensurePackedBlockDecoded(size_t lb_idx, size_t pb_idx);

    /// Ensure the position TurboPFor block at pos_block_idx is decoded.
    void ensurePosBlockDecoded(size_t lb_idx, UInt32 pos_block_idx);

    /// Get LargeBlockData for the given large block index.
    const LargeBlockData & getLargeBlockData(size_t lb_idx);

    LargePostingListReaderStreamPtr pst_stream;
    LargePostingListReaderStreamPtr pos_stream;
    LargePostingListReaderStreamPtr idx_stream;
    const ProjectionTokenInfo * info = nullptr;

    /// Cached last-decoded packed block (doc_ids + freqs from .pst)
    struct PackedBlockCache // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        size_t large_block = SIZE_MAX;
        size_t packed_block = SIZE_MAX;
        alignas(16) UInt32 doc_ids[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
        UInt32 freqs[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
        UInt32 count = 0;
    };

    /// Cached last-decoded position TurboPFor block (from .pos)
    struct PosBlockCache // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        size_t large_block = SIZE_MAX;
        UInt32 pos_block_idx = UINT32_MAX;
        UInt32 values[TURBOPFOR_BLOCK_SIZE]; // NOLINT(cppcoreguidelines-pro-type-member-init)
        UInt32 count = 0;
    };

    PackedBlockCache pb_cache;
    PosBlockCache pos_cache;

    /// Per-doc iteration state (set by seekDoc, advanced by nextPosition)
    struct DocPosState
    {
        size_t lb_idx = SIZE_MAX;
        UInt32 freq = 0;
        UInt32 consumed = 0;
        UInt32 cur_pos_block = 0;
        UInt32 cur_offset = 0;
        UInt32 last_position = 0;
        bool is_first = true;
    };
    DocPosState doc_state;

    /// Decode buffers for stream reads (lazily initialized).
    std::optional<TurboPForBlockDecodeBuffer> pst_decode_buf;
    std::optional<TurboPForBlockDecodeBuffer> pos_decode_buf;
};

using PositionCursorPtr = std::shared_ptr<PositionCursor>;

}
