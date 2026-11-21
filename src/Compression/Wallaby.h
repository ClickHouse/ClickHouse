#pragma once

#include <base/types.h>

/** Wallaby: lossless compression of `Float32` and `Float64` columns.
  * The format and the algorithm are described in Wallaby.cpp.
  */
namespace DB::Wallaby
{

/// Compresses `source_size` bytes of little-endian floats of the given width (4 or 8 bytes).
/// Writes at most `source_size + 1` bytes to `dest` and returns the number of bytes written.
UInt32 compress(const char * source, UInt32 source_size, char * dest, UInt8 float_width);

/// Decompresses a block produced by `compress` into exactly `uncompressed_size` bytes.
/// The float width is taken from the block. Throws `CANNOT_DECOMPRESS` if the block is malformed.
void decompress(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);

}
