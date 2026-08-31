#pragma once

#include "config.h"

#if USE_ARROW

#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <cstdint>
#include <utility>
#include <vector>

namespace DB::ArrowIPC
{

/// Arrow IPC per-buffer body compression codec (matches flatbuf::CompressionType).
enum class CompressionCodec : UInt8
{
    Lz4Frame = 0,
    Zstd = 1,
};

/// An upper bound on what `src` can decompress to. `exact` distinguishes a size the codec pledges
/// (so a prefix differing from it is forged) from a bound derived from the payload's structure (so
/// only a prefix exceeding it is).
struct FrameContentBound
{
    UInt64 size;
    bool exact;
};

/// The bound the codec frames of the whole of `src` imply. Throws INCORRECT_DATA when the codec
/// cannot parse `src` at all.
FrameContentBound frameContentBound(CompressionCodec codec, const char * src, size_t size);

/// Compresses Arrow IPC buffers, reusing one codec context across all buffers (creating a fresh
/// context per buffer — as a one-shot call would — dominates the cost when there are many buffers).
class Compressor
{
public:
    ~Compressor();
    /// Returns the compressed bytes (LZ4 frame or ZSTD frame, as Arrow expects).
    PODArray<char> compress(CompressionCodec codec, const char * src, size_t size, int level);

private:
    void * zstd_ctx = nullptr;
    void * lz4_ctx = nullptr;
};

/// Decompresses Arrow IPC buffers, reusing one codec context across all buffers.
class Decompressor
{
public:
    ~Decompressor();
    /// Decompresses `compressed` into exactly `uncompressed_size` bytes at `dst`.
    void decompress(CompressionCodec codec, const char * compressed, size_t compressed_size, char * dst, size_t uncompressed_size);

private:
    void * zstd_ctx = nullptr;
    void * lz4_ctx = nullptr;
};

/// Compresses each input buffer independently into `out[i]`, in parallel across the shared format
/// thread pool when the work is large enough (empty inputs yield empty outputs). Mirrors the Apache
/// Arrow writer, whose codec is multi-threaded; compression CPU is the dominant cost here.
void compressBuffersParallel(
    CompressionCodec codec, int level,
    const VectorWithMemoryTracking<std::pair<const char *, size_t>> & inputs, VectorWithMemoryTracking<PODArray<char>> & out);

/// One buffer to decompress into a pre-sized destination slot.
struct DecompressJob
{
    const char * src = nullptr;
    size_t src_size = 0;
    char * dst = nullptr;
    size_t dst_size = 0;
};

/// Decompresses each job into its destination, in parallel across the shared format thread pool when
/// the work is large enough. Destinations must be non-overlapping.
void decompressBuffersParallel(CompressionCodec codec, const VectorWithMemoryTracking<DecompressJob> & jobs);

}

#endif
