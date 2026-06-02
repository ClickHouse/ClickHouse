#pragma once

#include <base/types.h>

#include <Common/PODArray.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace DB
{

/// Immutable, decoded metadata of one segment of a compressed (bitpacked) posting list.
///
/// Built once per (token, segment) in `PostingListCursor::prepareSegment` and memoized on the
/// shared `MergeTreeIndexGranuleText` (one granule per part, reused by all parallel read tasks).
/// Per-task cursors hold non-owning views into this shared data, so the segment's payload and
/// per-block index are read from disk and parsed only once instead of once per read task.
///
/// All fields are write-once (at build time) and read-only afterwards, so the structure is safe
/// to share across threads without synchronization.
struct PostingListSegment
{
    /// Bulk-loaded compressed payload of the segment: bytes [header_end, index_section_start).
    std::vector<uint8_t> payload_buffer;

    /// Per-packed-block index (parallel arrays), enabling O(log N) advance within the segment:
    ///   block_last_row_ids[j] — last row_id of packed block j
    ///   block_offsets[j]      — byte offset of packed block j within `payload_buffer`
    std::vector<UInt32> block_last_row_ids;
    std::vector<UInt64> block_offsets;

    /// Total doc count in this segment.
    UInt32 segment_doc_count = 0;
    /// First row_id of the segment (delta base for the first block).
    UInt32 segment_first_row_id = 0;
    /// Total packed blocks, including the (possibly shorter) tail block.
    size_t block_count = 0;
    /// Element count of the tail block (< BLOCK_SIZE), 0 if the segment is block-aligned.
    size_t tail_size = 0;

    size_t bytesAllocated() const
    {
        return payload_buffer.capacity() * sizeof(uint8_t)
            + block_last_row_ids.capacity() * sizeof(UInt32)
            + block_offsets.capacity() * sizeof(UInt64);
    }
};

using PostingListSegmentPtr = std::shared_ptr<const PostingListSegment>;

/// A flattened, sorted array of posting list row ids. Used to back the lazy "prebuilt" cursor over
/// the analyzer-folded postings of eagerly-read tokens (built once via `toUint32Array`, then shared).
/// `PaddedPODArray` (not `std::vector`) so the cursor's vectorized linear scans may over-read past the
/// last element into the trailing SIMD padding without a separate bounds guard.
using FlatPostingsPtr = std::shared_ptr<const PaddedPODArray<UInt32>>;

}
