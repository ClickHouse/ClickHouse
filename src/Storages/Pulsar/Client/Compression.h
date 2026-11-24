#pragma once

#include <cstddef>
#include <string>

namespace DB::Pulsar
{

/// The largest message this client will allocate for decompression. The uncompressed size is taken
/// from the metadata written by the producer, so a tiny compressed frame could otherwise ask the
/// consumer to allocate gigabytes. Messages above this limit are rejected.
inline constexpr size_t MAX_UNCOMPRESSED_MESSAGE_SIZE = 256 * 1024 * 1024;

/// Decompress `size` bytes at `data` into `out`, which will hold exactly `uncompressed_size` bytes on
/// success. `compression_type` is a `pulsar.proto.CompressionType` value (NONE/LZ4/ZLIB/ZSTD/SNAPPY).
/// Returns false if the codec is unknown, the size is implausible, or the data does not decode to
/// exactly `uncompressed_size` bytes. Never throws, so a malformed message cannot escape the read loop.
bool decompress(int compression_type, const char * data, size_t size, size_t uncompressed_size, std::string & out);

}
