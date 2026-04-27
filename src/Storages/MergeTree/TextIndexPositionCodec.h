#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <vector>

namespace DB
{

/// Codec for encoding/decoding Roaringish 96-bit position lists.
///
/// Format:
///   [VarUInt: count]
///   [count x 12 bytes: (doc_id:u32, group:u32, bitmap:u32) in little-endian]
///
/// No compression is applied. The position data is read with a small number
/// of seeks (one per token per part), so raw storage minimizes decode latency.
class TextIndexPositionCodec
{
public:
    /// Writes a sorted array of RoaringishEntry values.
    /// Returns the number of bytes written.
    static void encode(const std::vector<RoaringishEntry> & entries, WriteBuffer & out);

    /// Reads into a vector of RoaringishEntry values.
    static void decode(ReadBuffer & in, std::vector<RoaringishEntry> & entries);
};

}
