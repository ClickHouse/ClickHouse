#pragma once

#include <cstddef>
#include <cstdint>

namespace DB::Pulsar
{

/// CRC32C (Castagnoli) over a byte range. The Pulsar binary protocol protects the metadata and
/// payload of every message with this checksum. A plain, portable table-driven implementation is used:
/// message framing, not checksumming, is the hot path.
uint32_t crc32c(const char * data, size_t size);

}
