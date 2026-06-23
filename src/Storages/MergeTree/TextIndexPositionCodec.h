#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <span>

namespace DB
{

/// Codec for encoding/decoding Roaringish 96-bit position lists, selected by the text
/// index `positions_encoding` parameter:
///
///   Raw  (positions_encoding='none', default):
///     [VarUInt: count][count x 12 bytes: (doc_id:u32, group:u32, bitmap:u32) little-endian]
///     No compression; minimal decode latency.
///
///   Pfor (positions_encoding='pfor'):
///     [VarUInt: count][VarUInt: payload_bytes][doc lane][group lane][bitmap lane]
///     The three UInt32 lanes are bit-packed with the PFor codec (clean-room PForDelta, Compression/PFor.h):
///     doc with integrated delta (non-decreasing across sorted entries), group and bitmap
///     plain. Smaller .pos files at the cost of a decode pass.
class TextIndexPositionCodec
{
public:
    enum class Encoding : UInt8
    {
        Raw = 0,
        Pfor = 1,
    };

    /// Reusable scratch for the Pfor decode path, owned by the caller and reused across
    /// decode() calls so the hot path does no per-call heap allocation (ClickHouse idiom;
    /// cf. MergeTreeReaderTextIndex::indices_buffer). PaddedPODArray also gives the SIMD
    /// pfor kernels safe trailing padding and skips value-initialization on resize.
    /// Unused by the Raw encoding.
    struct DecodeScratch
    {
        PaddedPODArray<char> payload;
        PaddedPODArray<UInt32> doc;
        PaddedPODArray<UInt32> group;
        PaddedPODArray<UInt32> bitmap;
    };

    /// Maps the `positions_encoding` argument value ("none"/"pfor") to an Encoding.
    /// Throws BAD_ARGUMENTS on an unknown value.
    static Encoding parseEncoding(const String & name);

    /// Writes a sorted array of RoaringishEntry values using the given encoding.
    static void encode(std::span<const RoaringishEntry> entries, WriteBuffer & out, Encoding encoding);

    /// Reads into a memory-tracked array of RoaringishEntry values (the merge path).
    static void decode(ReadBuffer & in, PODArray<RoaringishEntry> & entries, Encoding encoding, DecodeScratch & scratch);

    /// Reads into struct-of-arrays form (the query/phrase-search path). For Pfor this
    /// decodes the three columnar lanes straight into pl.doc/group/bitmap with no temp
    /// lane buffers and no SoA<->AoS interleave (just the compressed payload is staged in
    /// `payload_scratch`, reused across calls). For Raw it de-interleaves the stored
    /// entries into the three arrays.
    static void decode(ReadBuffer & in, PositionList & pl, Encoding encoding, PaddedPODArray<char> & payload_scratch);
};

}
