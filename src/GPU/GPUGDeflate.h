#pragma once

#include "config.h"

#if USE_GPU

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

namespace DB::GPU
{

/// A block of the `GDeflate` codec, see `CompressionCodecGDeflate`: its bytes in chunks of `gdeflate_chunk_bytes`, the
/// last one shorter, each a GDeflate page of its own, which the device expands in parallel with the others. The block
/// holds the number of chunks, a little-endian 32-bit integer, then the compressed size of each as one, then the chunks,
/// each from a multiple of 4 bytes, as the device reads them.
constexpr size_t gdeflate_chunk_bytes = 64 * 1024;

/// A chunk of a block, see `forEachGDeflateChunk`.
struct GDeflateChunk
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    size_t decompressed_offset = 0;
    size_t decompressed_bytes = 0;
};

/// Calls `on_chunk` for each chunk of a block that expands to `uncompressed_bytes`. Throws if the block does not hold as
/// many chunks as its bytes make, or a chunk that reaches past the block.
void forEachGDeflateChunk(std::string_view block, size_t uncompressed_bytes, const std::function<void(const GDeflateChunk &)> & on_chunk);

}

#endif
