#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <vector>

namespace DB
{

/// Codec for encoding/decoding Roaringish UInt128 position lists.
///
/// Format:
///   [VarUInt: count]
///   [count × UInt128: raw entries as (high UInt64, low UInt64) pairs]
///
/// No compression is applied. The position data is read with a small number
/// of seeks (one per token per part), so raw storage minimizes decode latency.
class TextIndexPositionCodec
{
public:
    /// Writes a sorted array of RoaringishEntry values.
    /// Returns the number of bytes written.
    static size_t encode(const std::vector<RoaringishEntry> & entries, WriteBuffer & out);

    /// Reads into a vector of RoaringishEntry values.
    static void decode(ReadBuffer & in, std::vector<RoaringishEntry> & entries);
};

}
