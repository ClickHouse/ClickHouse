#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDecompression.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <optional>
#include <vector>

namespace DB::GPU
{

/// A run of a column of a part on the host, as MergeTree stores it: compressed blocks in pinned memory, which the device
/// expands. Marks need not be at blocks: the column takes `kept_bytes` of what it expands to after `skipped_bytes`.
struct CompressedHostColumn
{
    /// Each block from a multiple of 8 bytes, as nvcomp reads them.
    PinnedBuffer bytes;
    std::vector<CompressedBlock> blocks;
    /// Of the blocks of chains of `BitPacking`, what they unpack to.
    std::vector<BitPackingBlock> packed;
    /// Of the blocks, all of one codec; nothing while there are none.
    std::optional<GPUCodec> codec;
    size_t skipped_bytes = 0;
    size_t kept_bytes = 0;

    bool empty() const { return blocks.empty(); }

    size_t expandedBytes() const { return valueBytesOf(blocks, packed); }

    /// The last block is taken in part, so that no block can follow it.
    bool isCut() const { return skipped_bytes + kept_bytes != expandedBytes(); }

    void clear()
    {
        bytes.clear();
        blocks.clear();
        packed.clear();
        codec.reset();
        skipped_bytes = 0;
        kept_bytes = 0;
    }
};

}

#endif
