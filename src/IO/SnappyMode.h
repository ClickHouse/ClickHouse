#pragma once

#include <cstdint>

namespace DB
{

/// Controls which wire format is used for Snappy compression.
enum class SnappyMode : uint8_t
{
    Basic,   /// Hadoop snappy block format (legacy ClickHouse behavior)
    Framed,  /// Snappy framing format — the standard streaming format
};

}
