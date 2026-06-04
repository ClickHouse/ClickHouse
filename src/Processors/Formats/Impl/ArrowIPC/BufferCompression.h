#pragma once

#include "config.h"

#if USE_ARROW

#include <Common/PODArray.h>
#include <cstdint>

namespace DB::ArrowIPC
{

/// Arrow IPC per-buffer body compression codec (matches flatbuf::CompressionType).
enum class CompressionCodec : int8_t
{
    Lz4Frame = 0,
    Zstd = 1,
};

/// Compresses `src` and returns the compressed bytes (LZ4 frame or ZSTD frame, as Arrow expects).
PODArray<char> compressBuffer(CompressionCodec codec, const char * src, size_t size, int level);

/// Decompresses `compressed` into exactly `uncompressed_size` bytes at `dst`.
void decompressBuffer(CompressionCodec codec, const char * compressed, size_t compressed_size, char * dst, size_t uncompressed_size);

}

#endif
