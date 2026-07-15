#pragma once
#include <base/types.h>
#include <Common/PODArray.h>
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

    /// Total doc count in this segment.
    UInt32 doc_count = 0;
    /// First row_id of the segment (delta base for the first block).
    UInt32 first_row_id = 0;
    /// Total packed blocks, including the (possibly shorter) tail block.
    size_t block_count = 0;
    /// Element count of the tail block (< BLOCK_SIZE), 0 if the segment is block-aligned.
    size_t tail_size = 0;

    size_t bytesAllocated() const
    {
        return sizeof(*this)
            + payload_buffer.allocated_bytes()
            + block_last_row_ids.allocated_bytes()
            + block_offsets.allocated_bytes();
    }
};

using PostingListSegmentPtr = std::shared_ptr<const PostingListSegment>;
/// A flattened, sorted array of posting list row ids.
using FlatPostingsPtr = std::shared_ptr<const PaddedPODArray<UInt32>>;

}
