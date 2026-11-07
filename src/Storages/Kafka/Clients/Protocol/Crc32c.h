#pragma once

/// CRC32C (Castagnoli) — used by record-batch v2. A thin wrapper over the
/// contrib/crc32c library (hardware-accelerated on SSE4.2 and ARMv8).

#include <base/types.h>

#include <string_view>

namespace Kafka::Protocol
{

UInt32 crc32c(std::string_view data);

}
