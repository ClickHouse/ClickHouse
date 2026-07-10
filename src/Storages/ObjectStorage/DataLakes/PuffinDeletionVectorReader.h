#pragma once

#include <IO/ReadBuffer.h>
#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

/// Deserialize a deletion-vector-v1 blob (magic + CRC wrapper + roaring bitmap payload).
std::vector<UInt64> deserializeDeletionVectorV1Blob(std::string_view blob_bytes);

/// Read a deletion-vector-v1 blob from a Puffin file at the given offset and length.
std::vector<UInt64> readDeletionVectorFromPuffin(ReadBuffer & file, Int64 offset, Int64 length);

}
