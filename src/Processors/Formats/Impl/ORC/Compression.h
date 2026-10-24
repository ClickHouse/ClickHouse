#pragma once

#include <Processors/Formats/Impl/ORC/Proto.h>

#include <memory>
#include <string>

namespace DB::ORC
{

using Proto::CompressionKind;

/// Every compressed chunk of an ORC stream is prefixed with a 3-byte little-endian header:
/// `(chunk_length << 1) | is_original`, where `is_original` means the chunk is stored as is.
static constexpr size_t COMPRESSION_HEADER_SIZE = 3;

/// Decompresses one chunk into `dst`, which has room for `dst_capacity` bytes (the compression block
/// size of the file). Returns the decompressed size. Throws `INCORRECT_DATA` on malformed input,
/// including a chunk that decompresses to more than `dst_capacity` bytes.
size_t decompressChunk(CompressionKind kind, const char * src, size_t src_size, char * dst, size_t dst_capacity);

/// Compresses chunks of an ORC stream.
class Compressor
{
public:
    virtual ~Compressor() = default;

    /// Appends the compressed representation of `src` to `out` and returns its size,
    /// or appends nothing and returns 0 if it would not be smaller than the input.
    virtual size_t compress(const char * src, size_t src_size, std::string & out) = 0;
};

/// Returns nullptr for `NONE`.
std::unique_ptr<Compressor> createCompressor(CompressionKind kind);

String compressionKindToString(CompressionKind kind);

}
