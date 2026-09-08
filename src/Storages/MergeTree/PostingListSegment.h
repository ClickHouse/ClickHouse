#pragma once
#include <base/types.h>
#include <Common/PODArray.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <cstdint>
#include <memory>

namespace DB
{

/// Immutable, decoded metadata of one segment of a compressed (bitpacked) posting list.
/// Per-task cursors hold non-owning views, so it is parsed once and safe to share without synchronization.
struct PostingListSegment
{
    /// Bulk-loaded compressed payload of the segment: bytes [header_end, index_section_start).
    PaddedPODArray<uint8_t> payload_buffer;
    /// Per-packed-block index (parallel arrays), enabling O(log N) advance within the segment.
    /// Last row_id of packed block j
    PaddedPODArray<UInt32> block_last_row_ids;
    /// Byte offset of packed block j within payload_buffer
    PaddedPODArray<UInt64> block_offsets;
    /// Per-block block-max upper-bound (UB) inputs for BM25 scoring.
    PaddedPODArray<UInt8> block_min_dl_byte;
    /// Max saturating `(tf - 1)` of packed block j (`255` is the "max tf >= 256" sentinel).
    PaddedPODArray<UInt8> block_max_tf_minus_one;

    /// Total doc count in this segment.
    UInt32 doc_count = 0;
    /// First row_id of the segment (delta base for the first block).
    UInt32 first_row_id = 0;
    /// Total packed blocks, including the (possibly shorter) tail block.
    size_t block_count = 0;
    /// Element count of the tail block (< BLOCK_SIZE), 0 if the segment is block-aligned.
    size_t tail_size = 0;
    /// Per-segment block-max UB input for BM25 scoring.
    UInt8 min_dl_byte = 0xFF;
    /// Max saturating `(tf - 1)` across the segment (`255` is the "max tf >= 256" sentinel).
    UInt8 max_tf_minus_one = 0;
    /// Block codec used to compress this segment's packed blocks.
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::Bitpacking;

    size_t bytesAllocated() const
    {
        return sizeof(*this)
            + payload_buffer.allocated_bytes()
            + block_last_row_ids.allocated_bytes()
            + block_offsets.allocated_bytes()
            + block_min_dl_byte.allocated_bytes()
            + block_max_tf_minus_one.allocated_bytes();
    }
};

using PostingListSegmentPtr = std::shared_ptr<const PostingListSegment>;

}
