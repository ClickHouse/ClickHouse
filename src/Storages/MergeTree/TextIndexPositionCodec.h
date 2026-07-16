#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <span>
#include <vector>

namespace DB
{

/// Codec for per-token position lists in the .pos stream: Raw ('none') stores 12-byte little-endian (doc_id, group, bitmap) entries; Pfor ('pfor') bit-packs the three UInt32 lanes (doc lane delta-encoded).
///
/// Blob layout:
///     [VarUInt: count]
///     [VarUInt: num_segments]
///     num_segments x [VarUInt: first_doc delta][VarUInt: last_doc - first_doc][VarUInt: count][VarUInt: bytes]
///     num_segments x payload, back to back, each independently decodable
///
/// Segments are cut at doc_id boundaries (target SEGMENT_TARGET_ENTRIES), so phrase search decodes one segment at a time and skips segments by doc range without reading them.
class TextIndexPositionCodec
{
public:
    enum class Encoding : UInt8
    {
        Raw = 0,
        Pfor = 1,
    };

    /// Unit of decode memory and I/O skipping; only a single doc_id with more entries can exceed it.
    static constexpr size_t SEGMENT_TARGET_ENTRIES = 16 * 1024;

    /// One directory record; `offset` is absolute in the .pos stream.
    struct SegmentMeta
    {
        UInt32 first_doc = 0;
        UInt32 last_doc = 0;
        UInt32 count = 0;
        UInt64 offset = 0;
        UInt64 bytes = 0;
    };

    struct SegmentDirectory
    {
        UInt64 total_count = 0;
        std::vector<SegmentMeta> segments;
    };

    /// Caller-owned scratch reused across decodeAllSegments() calls (no per-token allocation).
    struct DecodeScratch
    {
        PaddedPODArray<char> payload;
        PositionList lanes;
    };

    /// Maps "none"/"pfor" to an Encoding; throws BAD_ARGUMENTS on an unknown value.
    static Encoding parseEncoding(const String & name);

    /// Writes a sorted array of RoaringishEntry values in the segmented layout.
    static void encode(std::span<const RoaringishEntry> entries, WriteBuffer & out, Encoding encoding);

    /// Reads the per-token directory fail-closed; the stream must be at `blob_offset` (the token's `position_offset`).
    static SegmentDirectory readSegmentDirectory(ReadBuffer & in, UInt64 blob_offset, UInt64 position_cardinality, size_t available_bytes);

    /// Decodes one segment payload into `pl`; the stream must be at `meta.offset`.
    static void decodeSegment(ReadBuffer & in, const SegmentMeta & meta, Encoding encoding, PositionList & pl, PaddedPODArray<char> & payload_scratch);

    /// Whole-list sequential decode (the merge path).
    static void decodeAllSegments(ReadBuffer & in, PODArray<RoaringishEntry> & entries, Encoding encoding, UInt64 position_cardinality, size_t available_bytes, DecodeScratch & scratch);
};

}
