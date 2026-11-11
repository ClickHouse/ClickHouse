#pragma once

#include "config.h"

#if USE_LIBDEFLATE

#include <IO/CompressionMethod.h>

#include <cstddef>

namespace DB::Libdeflate
{

/// One-shot DEFLATE-family codec (gzip / zlib formats) backed by libdeflate.
///
/// Use this when the whole input is already in memory and, for decompression, the exact
/// uncompressed size is known up front (e.g. a Parquet page or column chunk). libdeflate is
/// noticeably faster than the streaming zlib path and yields a better ratio at the same level,
/// but it has no streaming API. For streaming data of unbounded size use the Zlib*Buffer classes.
///
/// Only CompressionMethod::Gzip and CompressionMethod::Zlib are accepted; anything else throws.

/// Decompress exactly `uncompressed_size` bytes into `out` (capacity must be `uncompressed_size`).
/// Throws CANNOT_DECOMPRESS on malformed input or if the decompressed size differs.
void decompress(CompressionMethod method, const char * src, size_t src_size, char * out, size_t uncompressed_size);

/// Upper bound on the compressed size of `src_size` input bytes at the given level (1..12).
size_t compressBound(CompressionMethod method, int level, size_t src_size);

/// Compress `src` into `out` (capacity `out_capacity`, which must be >= compressBound).
/// Returns the number of bytes written. Throws CANNOT_COMPRESS if `out_capacity` is insufficient.
size_t compress(CompressionMethod method, int level, const char * src, size_t src_size, char * out, size_t out_capacity);

}

#endif
