#pragma once

#include <cstdint>

/** Common Defines */

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL    /// 1GB

#define QUICKLZ_ADDITIONAL_SPACE 400
#define COMPRESSED_BLOCK_HEADER_SIZE 9


namespace DB
{

/** Compression method */
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4 = 1,
    LZ4HC = 2,        /// The format is the same as for LZ4. The difference is only in compression.
    ZSTD = 3,         /// Experimental algorithm: https://github.com/Cyan4973/zstd
    NONE = 4,         /// No compression
};

/** The compressed block format is as follows:
  *
  * The first 16 bytes are the checksum from all other bytes of the block. Now only CityHash128 is used.
  * In the future, you can provide other checksums, although it will not be possible to make them different in size.
  *
  * The next byte specifies the compression algorithm. Then everything depends on the algorithm.
  *
  * The first 4 options are compatible with QuickLZ level 1.
  * That is, if the value of the first byte is < 4, it is enough to use qlz_level1_decompress function to decompress.
  *
  * 0x00 - uncompressed data, small block. Next, one byte - compressed data size, including header; one byte - uncompressed data size.
  * 0x01 - compressed data, QuickLZ level 1, small block. Then two bytes are similar.
  * 0x02 - uncompressed data, large block. Then 4 bytes - compressed data size, including header; 4 bytes uncompressed data size.
  * 0x03 - compressed data, QuickLZ level 1, large block. Then 8 bytes are similar.
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
    NONE     = 0x02,
    LZ4      = 0x82,
    ZSTD     = 0x90,
};

}
