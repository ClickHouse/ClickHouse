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

/// A run of a column of a part on the host, as MergeTree stores it: compressed blocks in pinned memory, or the bytes the
/// CPU decompressed for a codec the device lacks. Marks need not be at blocks: the column takes `kept_bytes` of what it
/// expands to after `skipped_bytes`.
struct CompressedHostColumn
{
    /// Each block from a multiple of 8 bytes, as nvcomp reads them.
    PinnedBuffer bytes;
    std::vector<CompressedBlock> blocks;
    /// Nothing for decompressed bytes.
    std::optional<GPUCodec> codec;
    size_t skipped_bytes = 0;
    size_t kept_bytes = 0;

    bool isCompressed() const { return codec.has_value(); }

    bool empty() const { return bytes.empty(); }

    size_t expandedBytes() const { return isCompressed() ? decompressedBytesOf(blocks) : bytes.size(); }

    /// The last block is taken in part, so that no block can follow it.
    bool isCut() const { return skipped_bytes + kept_bytes != expandedBytes(); }

    void clear()
    {
        bytes.clear();
        blocks.clear();
        codec.reset();
        skipped_bytes = 0;
        kept_bytes = 0;
    }
};

}

#endif
