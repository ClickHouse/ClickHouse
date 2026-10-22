#pragma once

#include "config.h"

#if USE_GPU

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

namespace DB::GPU
{

/** A block of the `GDeflate` codec: its bytes in chunks of `gdeflate_chunk_bytes`, the last one shorter, each compressed by
  * nvcomp's GDeflate on its own, which the device expands in parallel. A chunk of a block of integers may be packed
  * before it is compressed: each integer as its difference from the least of the chunk, in as many bits as the largest
  * difference takes, which the device unpacks after it expands the chunk.
  *
  * The block holds the number of chunks, a little-endian 32-bit integer, then a descriptor of each chunk, then the chunks,
  * each from a multiple of 4 bytes, as the device reads them. The descriptor is the size of the compressed chunk, a
  * 32-bit integer; where the highest bit of the number of chunks is set, it is followed by the bits each integer is packed
  * in, 0 for a chunk that is not packed, the size of an integer, 2 bytes to spare and the least integer, as 64 bits.
  */
constexpr size_t gdeflate_chunk_bytes = 64 * 1024;

/// A chunk of a block, see `forEachGDeflateChunk`.
struct GDeflateChunk
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    /// What the chunk takes once expanded, and once its integers are unpacked.
    size_t expanded_bytes = 0;
    size_t decompressed_bytes = 0;
    /// 0 for a chunk that is not packed.
    uint32_t packed_bits = 0;
    uint32_t element_size = 0;
    uint64_t reference = 0;
};

/// The most a block of `uncompressed_bytes` takes.
size_t gdeflateMaxCompressedBytes(size_t uncompressed_bytes, int algorithm);

/// Compresses on the device, with GDeflate's `algorithm`, from 0, the fastest, to 5, the densest. Of integers of
/// `element_size` bytes, signed or not, a chunk is packed where that makes it compress to fewer bytes; `element_size` 0 packs
/// nothing. Returns the bytes written to `dest`, of at least `gdeflateMaxCompressedBytes`.
size_t gdeflateCompress(const char * source, size_t source_bytes, char * dest, int algorithm, uint32_t element_size, bool is_signed);

/// Expands a block on the device into `dest`, of `uncompressed_bytes`.
void gdeflateDecompress(const char * source, size_t source_bytes, char * dest, size_t uncompressed_bytes);

/// Calls `on_chunk` for each chunk of a block that expands to `uncompressed_bytes`. Throws if the block does not hold as
/// many chunks as its bytes make, or a chunk that reaches past the block.
void forEachGDeflateChunk(std::string_view block, size_t uncompressed_bytes, const std::function<void(const GDeflateChunk &)> & on_chunk);

}

#endif
