#pragma once

/// Compression codecs for record batches (attributes bits 0-2). Thin wrappers
/// over the contrib libraries: zlib-ng (gzip), lz4 (LZ4 Frame), zstd, and
/// optionally snappy with the xerial stream framing Kafka's Java client uses.

#include <Storages/Kafka/Clients/Protocol/Records.h>
#include <Storages/Kafka/Clients/Types.h>

#include <string_view>

namespace Kafka::Protocol
{

/// True when the codec is available in this build (snappy is optional).
bool compressionSupported(CompressionType type);

/// Compress a record-batch payload. level -1 = the codec's default level.
Bytes compress(CompressionType type, std::string_view data, int level = -1);

/// Decompress a record-batch payload. Throws ErrorKind::NotSupported for
/// codecs disabled at build time and ErrorKind::Protocol for corrupt data.
Bytes decompress(CompressionType type, std::string_view data);

}
