#pragma once

#include <cstdint>

/** Common defines for compression */

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL    /// 1GB

/** one byte for method, 4 bytes for compressed size, 4 bytes for uncompressed size */
#define COMPRESSED_BLOCK_HEADER_SIZE 9

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
    DeflateQpl      = 0x99,
    GCD             = 0x9a,
    ZSTD_QPL        = 0x9b,
    SZ3             = 0x9c,
};

}
