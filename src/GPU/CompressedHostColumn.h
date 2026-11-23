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

/// A run of a column of a part on the host, as MergeTree stores it, to be sent to the device and expanded there: its
/// compressed blocks one after another in pinned memory, or, of a codec the device does not expand, the bytes the CPU
/// has decompressed. It becomes a column on the device in `GPUBatchUploader`.
///
/// The marks read need not start or end at a block, so of what the run expands to, the column takes `kept_bytes` after
/// `skipped_bytes`.
struct CompressedHostColumn
{
    /// The blocks, each from a multiple of 8 bytes, as nvcomp reads them, or the decompressed bytes.
    PinnedBuffer bytes;
    /// Where each block is in `bytes`, and what it expands to. Empty for decompressed bytes.
    std::vector<CompressedBlock> blocks;
    /// The codec of the blocks; nothing for decompressed bytes.
    std::optional<GPUCodec> codec;
    size_t skipped_bytes = 0;
    size_t kept_bytes = 0;

    bool isCompressed() const { return codec.has_value(); }

    bool empty() const { return bytes.empty(); }

    /// The bytes the run expands to on the device.
    size_t expandedBytes() const { return isCompressed() ? decompressedBytesOf(blocks) : bytes.size(); }

    /// The last block keeps less than all of its bytes, so that the next one cannot follow it.
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
