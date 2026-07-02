#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/transformEndianness.h>

#include <Compression/PFor.h>

#include <bit>
#include <cstring>
#include <span>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
}

namespace
{
/// Entries are little-endian on disk; swap each lane to/from native (compiles away on little-endian hosts).
[[maybe_unused]] void transformEntryEndianness(RoaringishEntry & e)
{
    transformEndianness<std::endian::little>(e.doc_id);
    transformEndianness<std::endian::little>(e.group);
    transformEndianness<std::endian::little>(e.bitmap);
}

/// ---- Raw: count + the entries' little-endian bytes ----

void encodeRaw(std::span<const RoaringishEntry> entries, WriteBuffer & out)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = entries.size();
    writeVarUInt(count, out);
    if (count == 0)
        return;

    if constexpr (std::endian::native == std::endian::little)
        out.write(reinterpret_cast<const char *>(entries.data()), count * sizeof(RoaringishEntry));
    else
        for (RoaringishEntry e : entries)
        {
            transformEntryEndianness(e);
            out.write(reinterpret_cast<const char *>(&e), sizeof(e));
        }
}

void decodeRaw(ReadBuffer & in, PODArray<RoaringishEntry> & entries)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = 0;
    readVarUInt(count, in);
    if (count == 0)
        return;

    entries.resize(count);
    /// readBigStrict reads the bulk payload straight into the destination, skipping the buffer copy.
    in.readBigStrict(reinterpret_cast<char *>(entries.data()), count * sizeof(RoaringishEntry));
    if constexpr (std::endian::native != std::endian::little)
        for (auto & e : entries)
            transformEntryEndianness(e);
}

void decodeRawSoA(ReadBuffer & in, PositionList & pl, PaddedPODArray<char> & scratch)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = 0;
    readVarUInt(count, in);
    if (count == 0)
        return;

    pl.resize(count);
    /// Bulk-read the AoS payload in one pass, then de-interleave into the lanes.
    const size_t bytes = count * sizeof(RoaringishEntry);
    scratch.resize(bytes);
    in.readBigStrict(scratch.data(), bytes);

    const char * base = scratch.data();
    for (size_t i = 0; i < count; ++i)
    {
        RoaringishEntry entry{};
        memcpy(&entry, base + i * sizeof(RoaringishEntry), sizeof(RoaringishEntry));
        if constexpr (std::endian::native != std::endian::little)
            transformEntryEndianness(entry);
        pl.doc[i] = entry.doc_id;
        pl.group[i] = entry.group;
        pl.bitmap[i] = entry.bitmap;
    }
}

/// ---- Pfor: the three UInt32 lanes bit-packed with the PFor codec (Compression/PFor.h) ----

/// doc_id delta-packed (d0, non-decreasing), group/bitmap plain; the three lane blobs are concatenated into one length-prefixed payload.
void encodePfor(std::span<const RoaringishEntry> entries, WriteBuffer & out)
{
    const UInt64 count = entries.size();
    writeVarUInt(count, out);

    if (count == 0)
        return;

    std::vector<UInt32> doc(count);
    std::vector<UInt32> grp(count);
    std::vector<UInt32> bm(count);
    for (size_t i = 0; i < count; ++i)
    {
        doc[i] = entries[i].doc_id;
        grp[i] = entries[i].group;
        bm[i] = entries[i].bitmap;
    }

    /// PFor works on uint8_t buffers (ClickHouse UInt8 is char8_t), so the byte buffer must be uint8_t.
    std::vector<uint8_t> payload(3 * PFor::maxCompressedBytes<UInt32>(count));
    size_t off = 0;
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(doc), PFor::Delta::d0, payload.data() + off);
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(grp), PFor::Delta::none, payload.data() + off);
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(bm), PFor::Delta::none, payload.data() + off);

    writeVarUInt(static_cast<UInt64>(off), out);
    out.write(reinterpret_cast<const char *>(payload.data()), off);
}

/// Decode one PFor lane fail-closed: throw CORRUPTED_DATA if truncated or malformed (decodeBlocks returns 0); returns the position past the lane.
const uint8_t * decodePforLane(const uint8_t * p, const uint8_t * end, UInt64 count, PFor::Delta mode, UInt32 * out)
{
    const size_t consumed = PFor::decodeBlocks<UInt32>(p, count, mode, out, end);
    if (consumed == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions (pfor): truncated or malformed lane");
    return p + consumed;
}

void decodePfor(ReadBuffer & in, PODArray<RoaringishEntry> & entries, TextIndexPositionCodec::DecodeScratch & scratch)
{
    UInt64 count = 0;
    readVarUInt(count, in);

    if (count == 0)
        return;

    UInt64 payload_bytes = 0;
    readVarUInt(payload_bytes, in);

    /// Reused buffers; PaddedPODArray::resize skips value-init and keeps trailing SIMD padding.
    scratch.payload.resize(payload_bytes);
    if (payload_bytes > 0)
        in.readStrict(scratch.payload.data(), payload_bytes);

    scratch.doc.resize(count);
    scratch.group.resize(count);
    scratch.bitmap.resize(count);
    const uint8_t * const start = reinterpret_cast<const uint8_t *>(scratch.payload.data());
    const uint8_t * const end = start + payload_bytes;
    const uint8_t * p = start;
    p = decodePforLane(p, end, count, PFor::Delta::d0, scratch.doc.data());
    p = decodePforLane(p, end, count, PFor::Delta::none, scratch.group.data());
    p = decodePforLane(p, end, count, PFor::Delta::none, scratch.bitmap.data());
    if (p != end)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions (pfor): payload not fully consumed ({} of {} bytes)",
            static_cast<size_t>(p - start), payload_bytes);

    entries.resize(count);
    for (size_t i = 0; i < count; ++i)
        entries[i] = RoaringishEntry{scratch.doc[i], scratch.group[i], scratch.bitmap[i]};
}

void decodePforSoA(ReadBuffer & in, PositionList & pl, PaddedPODArray<char> & payload)
{
    UInt64 count = 0;
    readVarUInt(count, in);
    if (count == 0)
        return;

    UInt64 payload_bytes = 0;
    readVarUInt(payload_bytes, in);

    payload.resize(payload_bytes);
    if (payload_bytes > 0)
        in.readStrict(payload.data(), payload_bytes);

    /// Decode the three columnar lanes straight into the SoA arrays (fail-closed on corrupt input).
    pl.resize(count);
    const uint8_t * const start = reinterpret_cast<const uint8_t *>(payload.data());
    const uint8_t * const end = start + payload_bytes;
    const uint8_t * p = start;
    p = decodePforLane(p, end, count, PFor::Delta::d0, pl.doc.data());
    p = decodePforLane(p, end, count, PFor::Delta::none, pl.group.data());
    p = decodePforLane(p, end, count, PFor::Delta::none, pl.bitmap.data());
    if (p != end)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions (pfor): payload not fully consumed ({} of {} bytes)",
            static_cast<size_t>(p - start), payload_bytes);
}

}

TextIndexPositionCodec::Encoding TextIndexPositionCodec::parseEncoding(const String & name)
{
    if (name == "none")
        return Encoding::Raw;
    if (name == "pfor")
        return Encoding::Pfor;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown text index positions_codec '{}', expected 'none' or 'pfor'", name);
}

void TextIndexPositionCodec::encode(std::span<const RoaringishEntry> entries, WriteBuffer & out, Encoding encoding)
{
    if (encoding == Encoding::Pfor)
        encodePfor(entries, out);
    else
        encodeRaw(entries, out);
}

void TextIndexPositionCodec::decode(ReadBuffer & in, PODArray<RoaringishEntry> & entries, Encoding encoding, DecodeScratch & scratch)
{
    if (encoding == Encoding::Pfor)
        decodePfor(in, entries, scratch);
    else
        decodeRaw(in, entries);
}

void TextIndexPositionCodec::decode(ReadBuffer & in, PositionList & pl, Encoding encoding, PaddedPODArray<char> & payload_scratch)
{
    if (encoding == Encoding::Pfor)
        decodePforSoA(in, pl, payload_scratch);
    else
        decodeRawSoA(in, pl, payload_scratch);
}

}
