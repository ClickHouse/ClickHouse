#pragma once

#include "config.h"

#if USE_GPU

#include <cstddef>
#include <functional>
#include <string_view>

namespace DB::GPU
{

/** A block of the `GDeflate` codec: its bytes in chunks of `gdeflate_chunk_bytes`, the last one shorter, each compressed by
  * nvcomp's GDeflate on its own, which the device expands in parallel. The block holds the number of chunks and the size
  * of each compressed chunk, as little-endian 32-bit integers, then the chunks, each from a multiple of 4 bytes, as the
  * device reads them.
  */
constexpr size_t gdeflate_chunk_bytes = 64 * 1024;

/// The most a block of `uncompressed_bytes` takes.
size_t gdeflateMaxCompressedBytes(size_t uncompressed_bytes, int algorithm);

/// Compresses on the device, with GDeflate's `algorithm`, from 0, the fastest, to 5, the densest. Returns the bytes
/// written to `dest`, of at least `gdeflateMaxCompressedBytes`.
size_t gdeflateCompress(const char * source, size_t source_bytes, char * dest, int algorithm);

/// Expands a block on the device into `dest`, of `uncompressed_bytes`.
void gdeflateDecompress(const char * source, size_t source_bytes, char * dest, size_t uncompressed_bytes);

/// Calls `on_chunk` for each chunk of a block that expands to `uncompressed_bytes`: where it starts in the block, its
/// compressed and its expanded size. Throws if the block does not hold as many chunks as its bytes make.
void forEachGDeflateChunk(
    std::string_view block,
    size_t uncompressed_bytes,
    const std::function<void(size_t offset, size_t compressed_bytes, size_t decompressed_bytes)> & on_chunk);

}

#endif
