#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB::GPU
{

class DecompressionBatch;

/// The compressed blocks of a stream of a column in pinned memory, as MergeTree stores them, which the device expands.
/// A run of rows starts and ends at marks, not at blocks: of what the blocks expand to, the column takes `keptBytes`
/// after `skippedBytes`.
class CompressedHostColumn
{
public:
    /// Appends a block of compression `method`, `payload` after its header, which expands to `decompressed_bytes`, of
    /// which the column takes `kept_bytes` after `skipped_bytes`. Only the first block skips bytes, and no block follows
    /// one cut short. `reserve_bytes` is about what the whole run takes, to allocate once. Throws `NOT_IMPLEMENTED` on a
    /// codec the device does not expand.
    void append(UInt8 method, std::string_view payload, size_t decompressed_bytes, size_t skipped_bytes, size_t kept_bytes, size_t reserve_bytes);

    bool empty() const { return chunks.empty(); }

    /// What the blocks expand to, and what the column takes of it.
    size_t expandedBytes() const { return expanded_bytes; }
    size_t skippedBytes() const { return skipped_bytes; }
    size_t keptBytes() const { return kept_bytes; }

    void clear();

private:
    friend class DecompressionBatch;

    /// What one call of nvcomp expands: a block, or a chunk of a `GDeflate` one, at `offset` in `bytes`.
    struct Chunk
    {
        GPUCodec codec;
        size_t offset;
        size_t compressed_bytes;
        size_t decompressed_bytes;
    };

    bool isCut() const { return skipped_bytes + kept_bytes != expanded_bytes; }

    /// Each chunk from a multiple of 8 bytes, as nvcomp reads them.
    PinnedBuffer bytes;
    std::vector<Chunk> chunks;
    size_t expanded_bytes = 0;
    size_t skipped_bytes = 0;
    size_t kept_bytes = 0;
};

}

#endif
