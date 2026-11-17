#pragma once

#include <cstddef>
#include <cstdint>

/** The format of a block of the `BitPacking` codec, shared by the codec, which packs and unpacks on the host, and the
  * kernel that unpacks on the GPU, see `GPU::unpackStreams`.
  *
  * A block holds integers of one width, in chunks of `chunk_bytes` of them, the last one shorter. Each chunk is packed
  * as its least integer, the reference, and the difference of each integer from it in `bits` bits, from the lowest bit
  * of its first byte on: 0 bits for a chunk of one value, and `raw_bits` for integers of 8 bytes that differ by more than
  * `max_packed_bits` bits, which are stored as they are. The bytes at the end of the block that do not make a whole
  * integer, as a block of a column is cut by size, are the tail, stored as they are after the chunks.
  *
  * The block is a header of `header_bytes`: the version, the width of an integer, the number of bytes of the tail, a
  * byte to spare and the number of chunks, as a 32-bit integer; then a descriptor of `descriptor_bytes` per chunk:
  * `bits`, 7 bytes to spare and the reference as a 64-bit integer; then the packed bytes of each chunk, from a multiple
  * of 8 bytes; then the tail; then `padding_bytes` of zeros, so that an 8-byte read at any byte a chunk's bits are in
  * stays inside the block. All integers are little-endian.
  */
namespace DB::BitPackingFormat
{

constexpr uint8_t version = 1;
constexpr size_t chunk_bytes = 64 * 1024;
constexpr size_t header_bytes = 8;
constexpr size_t descriptor_bytes = 16;
constexpr size_t padding_bytes = 8;
/// The most bits a difference is packed in: with the 8 bytes read at its first byte, it ends within them from any bit.
constexpr uint32_t max_packed_bits = 56;
constexpr uint32_t raw_bits = 64;

constexpr size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + chunk_bytes - 1) / chunk_bytes;
}

constexpr size_t packedBytes(size_t count, uint32_t bits)
{
    return (count * bits + 7) / 8;
}

constexpr size_t paddedBytes(size_t bytes)
{
    return (bytes + 7) / 8 * 8;
}

constexpr size_t descriptorOffset(size_t chunk)
{
    return header_bytes + chunk * descriptor_bytes;
}

constexpr size_t dataOffset(size_t num_chunks)
{
    return header_bytes + num_chunks * descriptor_bytes;
}

}
