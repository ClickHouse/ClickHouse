#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <algorithm>
#include <span>
#include <vector>

namespace DB
{

/// Blocked candidate-driven positions codec (the text index positions layout).
///
/// One token's positions as per-document lists, chunked into blocks of BLOCK_DOCS consecutive
/// posting ranks. No doc ids and no freq lane in the stream: block b covers postings entries
/// [b*BLOCK_DOCS, (b+1)*BLOCK_DOCS), so a posting cursor's rank addresses a document directly and a
/// phrase query fetches only the blocks covering its candidate rows.
///
/// Per-token stream layout (at token_info.position_offset in the .pos substream):
///     [VarUInt: num_docs]
///     [VarUInt: num_blocks]                        (must equal ceil(num_docs / BLOCK_DOCS))
///     num_blocks x [VarUInt: payload_bytes]        (directory; one contiguous read, offsets by prefix sum)
///     num_blocks x payload, back to back
///
/// Block payload (docs_in_block = min(BLOCK_DOCS, num_docs - b*BLOCK_DOCS)):
///     [VarUInt: num_exceptions]
///     num_exceptions x [VarUInt: local_rank][VarUInt: freq]   (ascending local_rank; freq >= 2;
///                                                              a document's freq defaults to 1)
///     [PFor: total_positions UInt32 values]        (total_positions = docs_in_block + sum(freq - 1);
///                                                   within-document deltas in posting order, the
///                                                   first position of each document absolute)
///
/// All counts are validated fail-closed (CORRUPTED_DATA) before any allocation.
class TextIndexBlockedPositionsCodec
{
public:
    /// Posting ranks per block; baked into the format (changing it needs a version bump).
    static constexpr size_t BLOCK_DOCS = 128;

    /// Parsed per-token directory; block_offsets are absolute in the .pos stream (seek-ready).
    struct Directory
    {
        UInt64 num_docs = 0;
        /// block_offsets[b] .. block_offsets[b + 1] is block b's payload; size numBlocks() + 1.
        std::vector<UInt64> block_offsets;

        size_t numBlocks() const { return block_offsets.empty() ? 0 : block_offsets.size() - 1; }
        size_t docsInBlock(size_t block_idx) const
        {
            const UInt64 begin = block_idx * BLOCK_DOCS;
            return static_cast<size_t>(std::min<UInt64>(BLOCK_DOCS, num_docs - begin));
        }
    };

    /// Caller-owned scratch reused across block decodes (PaddedPODArray pads the PFor lanes).
    struct DecodeScratch
    {
        PaddedPODArray<char> payload;
        PaddedPODArray<UInt64> doc_offsets;
        PaddedPODArray<UInt32> values;
    };

    /// Encodes the writer's sorted RoaringishEntry accumulation as the blocked stream.
    static void encode(std::span<const RoaringishEntry> entries, WriteBuffer & out);

    /// Reads the directory (stream positioned at the token's `blob_offset` = position_offset).
    /// `expected_num_docs` (header cardinality) and `available_bytes` fail-close every declared size.
    static Directory readDirectory(ReadBuffer & in, UInt64 blob_offset, UInt64 expected_num_docs, size_t available_bytes);

    /// Decodes block `block_idx` (stream at dir.block_offsets[block_idx]); appends each requested
    /// local rank's positions to `positions` and its end offset to `offsets`.
    static void decodeBlock(
        ReadBuffer & in,
        const Directory & dir,
        size_t block_idx,
        std::span<const UInt32> local_ranks,
        PaddedPODArray<UInt32> & offsets,
        PaddedPODArray<UInt32> & positions,
        DecodeScratch & scratch);

    /// Whole-token sequential decode (merge path): every rank's positions, delimited by
    /// doc_offsets (size num_docs + 1).
    static void decodeAll(
        ReadBuffer & in,
        UInt64 expected_num_docs,
        size_t available_bytes,
        PaddedPODArray<UInt32> & doc_offsets,
        PaddedPODArray<UInt32> & positions,
        DecodeScratch & scratch);
};

}
