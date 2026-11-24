#pragma once

#include <cstdint>

/** Common defines for compression */

/** The maximum size of a compressed block as stored, i.e. the "compressed size" field of the block header
  * (the header and the compressed payload, without the checksum). The reader checks it before reading
  * the block into memory, and the writer refuses to write a larger block.
  *
  * A block is compressed from at most DBMS_MAX_DECOMPRESSED_SIZE bytes (see below), but compression
  * does not always make the data smaller: on incompressible input, codecs add their own overhead
  * (e.g. LZ4 worst case is slightly above the input size, `T64`, `Gorilla`, `FPC` and encryption codecs add
  * headers or per-value overhead, and a chain of codecs adds up). It is set to twice
  * DBMS_MAX_DECOMPRESSED_SIZE to leave ample headroom for that, so that any block a writer
  * produces from a maximum-sized input is still accepted by the reader, while the amount of memory
  * a single block can make the reader allocate stays bounded.
  */
constexpr uint64_t DBMS_MAX_COMPRESSED_SIZE = 0x20000000ULL;    /// 512 MiB

/** The decompressed size of a block is taken from the block header, and the buffer for it is
  * allocated before the block is decompressed, so it has to be bounded as well: a block that
  * declares a huge decompressed size makes the reader allocate that much from a tiny payload.
  * Blocks are written with `max_compress_block_size`, which is 1 MB by default, and never larger than this.
  */
constexpr uint64_t DBMS_MAX_DECOMPRESSED_SIZE = 0x10000000ULL;  /// 256 MiB

/** one byte for method, 4 bytes for compressed size, 4 bytes for uncompressed size */
constexpr uint8_t COMPRESSED_BLOCK_HEADER_SIZE = 9;

namespace DB
{

/** The compressed block format is as follows:
  *
  * The first 16 bytes are the checksum from all other bytes of the block. Now only CityHash128 is used.
  * In the future, you can provide other checksums, although it will not be possible to make them different in size.
  *
  * The next byte specifies the compression algorithm. Then everything depends on the algorithm.
  *
  * 0x82 - LZ4 or LZ4HC (they have the same format).
  *        Next 4 bytes - the size of the compressed data, taking into account the header; 4 bytes is the size of the uncompressed data.
  *
  * NOTE: Why is 0x82?
  * Originally only QuickLZ was used. Then LZ4 was added.
  * The high bit is set to distinguish from QuickLZ, and the second bit is set for compatibility,
  *  for the functions qlz_size_compressed, qlz_size_decompressed to work.
  * Although now such compatibility is no longer relevant.
  *
  * 0x90 - ZSTD
  *
  * All sizes are little endian.
  */

enum class CompressionMethodByte : uint8_t
{
    NONE            = 0x02,
    LZ4             = 0x82,
    ZSTD            = 0x90,
    Multiple        = 0x91,
    Delta           = 0x92,
    T64             = 0x93,
    DoubleDelta     = 0x94,
    Gorilla         = 0x95,
    AES_128_GCM_SIV = 0x96,
    AES_256_GCM_SIV = 0x97,
    FPC             = 0x98,
    /// DeflateQpl      = 0x99, /// Removed, don't reuse for another codec
    GCD             = 0x9a,
    /// ZSTD_QPL        = 0x9b, /// Removed, don't reuse for another codec
    ALP             = 0x9c,
    SZ3             = 0x9d,
    Quantized       = 0x9e,
    ZXC             = 0x9f,
    PCO             = 0xa0,
    GDeflate        = 0xa1,
};

}
