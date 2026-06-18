#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <span>

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
    static void encode(std::span<const RoaringishEntry> entries, WriteBuffer & out);

    /// Reads into a memory-tracked array of RoaringishEntry values (the merge path).
    static void decode(ReadBuffer & in, PODArray<RoaringishEntry> & entries);

    /// Reads into struct-of-arrays form (the query/phrase-search path): de-interleaves the
    /// on-disk AoS entries into pl.doc/group/bitmap.
    static void decode(ReadBuffer & in, PositionList & pl);
};

}
