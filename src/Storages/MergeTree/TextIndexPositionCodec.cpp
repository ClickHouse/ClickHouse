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

/// Checked before any resize(count) so a bogus on-disk count is rejected up front.
void checkPositionCount(UInt64 count, UInt64 expected_count)
{
    if (count != expected_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: stored entry count {} does not match index cardinality {}", count, expected_count);
}

/// Reject a declared size larger than the bytes remaining for this token, before any resize.
void checkFitsAvailable(UInt64 needed_bytes, size_t available_bytes)
{
    if (needed_bytes > available_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: declared size {} bytes exceeds {} bytes available for this token in the .pos stream",
            needed_bytes, available_bytes);
}

/// At least 1 byte per block per lane, at most three full lanes.
void checkPforPayloadBounds(UInt64 payload_bytes, UInt64 count)
{
    const size_t min_payload = 3 * ((count + PFor::BLOCK - 1) / PFor::BLOCK);
    const size_t max_payload = 3 * PFor::maxCompressedBytes<UInt32>(count);
    if (payload_bytes < min_payload || payload_bytes > max_payload)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions (pfor): payload size {} outside the valid range [{}, {}] for {} entries",
            payload_bytes, min_payload, max_payload, count);
}

/// Decode one PFor lane fail-closed: throw CORRUPTED_DATA if truncated or malformed (decodeBlocks returns 0); returns the position past the lane.
const uint8_t * decodePforLane(const uint8_t * p, const uint8_t * end, UInt64 count, PFor::Delta mode, UInt32 * out)
{
    const size_t consumed = PFor::decodeBlocks<UInt32>(p, count, mode, out, end);
    if (consumed == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions (pfor): truncated or malformed lane");
    return p + consumed;
}

/// Cuts at the first doc_id change at/after the target size, so segments never split a doc_id.
std::vector<std::pair<size_t, size_t>> computeSegmentBounds(std::span<const RoaringishEntry> entries)
{
    std::vector<std::pair<size_t, size_t>> bounds;
    const size_t count = entries.size();
    bounds.reserve(count / TextIndexPositionCodec::SEGMENT_TARGET_ENTRIES + 1);

    size_t begin = 0;
    for (size_t i = 1; i < count; ++i)
    {
        if (i - begin >= TextIndexPositionCodec::SEGMENT_TARGET_ENTRIES && entries[i].doc_id != entries[i - 1].doc_id)
        {
            bounds.emplace_back(begin, i);
            begin = i;
        }
    }

    bounds.emplace_back(begin, count);
    return bounds;
}

/// The doc lane restarts its d0 delta base per segment, making segments independently decodable.
size_t encodePforSegment(std::span<const RoaringishEntry> entries, std::vector<UInt32> & doc, std::vector<UInt32> & grp, std::vector<UInt32> & bm, uint8_t * out)
{
    const size_t count = entries.size();
    doc.resize(count);
    grp.resize(count);
    bm.resize(count);
    for (size_t i = 0; i < count; ++i)
    {
        doc[i] = entries[i].doc_id;
        grp[i] = entries[i].group;
        bm[i] = entries[i].bitmap;
    }

    size_t off = 0;
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(doc), PFor::Delta::d0, out + off);
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(grp), PFor::Delta::none, out + off);
    off += PFor::encodeBlocks<UInt32>(std::span<const UInt32>(bm), PFor::Delta::none, out + off);
    return off;
}

void writeRawSegment(std::span<const RoaringishEntry> entries, WriteBuffer & out)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    if constexpr (std::endian::native == std::endian::little)
        out.write(reinterpret_cast<const char *>(entries.data()), entries.size() * sizeof(RoaringishEntry));
    else
        for (RoaringishEntry e : entries)
        {
            transformEntryEndianness(e);
            out.write(reinterpret_cast<const char *>(&e), sizeof(e));
        }
}

void decodeRawSegment(ReadBuffer & in, const TextIndexPositionCodec::SegmentMeta & meta, PositionList & pl, PaddedPODArray<char> & scratch)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    pl.resize(meta.count);
    scratch.resize(meta.bytes);
    in.readBigStrict(scratch.data(), meta.bytes);

    const char * base = scratch.data();
    for (size_t i = 0; i < meta.count; ++i)
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

void decodePforSegment(ReadBuffer & in, const TextIndexPositionCodec::SegmentMeta & meta, PositionList & pl, PaddedPODArray<char> & scratch)
{
    scratch.resize(meta.bytes);
    in.readStrict(scratch.data(), meta.bytes);

    pl.resize(meta.count);
    const uint8_t * const start = reinterpret_cast<const uint8_t *>(scratch.data());
    const uint8_t * const end = start + meta.bytes;
    const uint8_t * p = start;
    p = decodePforLane(p, end, meta.count, PFor::Delta::d0, pl.doc.data());
    p = decodePforLane(p, end, meta.count, PFor::Delta::none, pl.group.data());
    p = decodePforLane(p, end, meta.count, PFor::Delta::none, pl.bitmap.data());
    if (p != end)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions (pfor): segment payload not fully consumed ({} of {} bytes)",
            static_cast<size_t>(p - start), static_cast<size_t>(meta.bytes));
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
    const UInt64 count = entries.size();
    writeVarUInt(count, out);
    if (count == 0)
        return;

    const auto bounds = computeSegmentBounds(entries);
    writeVarUInt(static_cast<UInt64>(bounds.size()), out);

    /// The directory precedes the payloads, so pfor segments are staged in memory to learn their sizes.
    std::vector<uint8_t> pfor_payload;
    std::vector<size_t> segment_bytes(bounds.size());

    if (encoding == Encoding::Pfor)
    {
        std::vector<UInt32> doc;
        std::vector<UInt32> grp;
        std::vector<UInt32> bm;
        size_t reserve_bytes = 0;
        for (const auto & [begin, end] : bounds)
            reserve_bytes += 3 * PFor::maxCompressedBytes<UInt32>(end - begin);
        pfor_payload.resize(reserve_bytes);

        size_t off = 0;
        for (size_t s = 0; s < bounds.size(); ++s)
        {
            const auto [begin, end] = bounds[s];
            segment_bytes[s] = encodePforSegment(entries.subspan(begin, end - begin), doc, grp, bm, pfor_payload.data() + off);
            off += segment_bytes[s];
        }
        pfor_payload.resize(off);
    }
    else
    {
        for (size_t s = 0; s < bounds.size(); ++s)
            segment_bytes[s] = (bounds[s].second - bounds[s].first) * sizeof(RoaringishEntry);
    }

    UInt32 prev_first_doc = 0;
    for (size_t s = 0; s < bounds.size(); ++s)
    {
        const auto [begin, end] = bounds[s];
        const UInt32 first_doc = entries[begin].doc_id;
        const UInt32 last_doc = entries[end - 1].doc_id;

        writeVarUInt(first_doc - prev_first_doc, out);
        writeVarUInt(last_doc - first_doc, out);
        writeVarUInt(static_cast<UInt64>(end - begin), out);
        writeVarUInt(static_cast<UInt64>(segment_bytes[s]), out);
        prev_first_doc = first_doc;
    }

    if (encoding == Encoding::Pfor)
    {
        out.write(reinterpret_cast<const char *>(pfor_payload.data()), pfor_payload.size());
    }
    else
    {
        for (const auto & [begin, end] : bounds)
            writeRawSegment(entries.subspan(begin, end - begin), out);
    }
}

TextIndexPositionCodec::SegmentDirectory TextIndexPositionCodec::readSegmentDirectory(ReadBuffer & in, UInt64 blob_offset, UInt64 position_cardinality, size_t available_bytes)
{
    const size_t count_before = in.count();

    SegmentDirectory dir;
    readVarUInt(dir.total_count, in);
    checkPositionCount(dir.total_count, position_cardinality);
    if (dir.total_count == 0)
        return dir;

    UInt64 num_segments = 0;
    readVarUInt(num_segments, in);
    /// Every segment holds >= 1 entry and its directory record takes >= 4 bytes.
    if (num_segments == 0 || num_segments > dir.total_count || num_segments * 4 > available_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: segment count {} is invalid for {} entries in {} available bytes",
            num_segments, dir.total_count, available_bytes);

    dir.segments.resize(num_segments);

    UInt64 sum_count = 0;
    UInt64 sum_bytes = 0;
    UInt32 prev_first_doc = 0;
    UInt32 prev_last_doc = 0;

    for (size_t s = 0; s < num_segments; ++s)
    {
        UInt64 first_doc_delta = 0;
        UInt64 last_doc_span = 0;
        UInt64 seg_count = 0;
        UInt64 seg_bytes = 0;
        readVarUInt(first_doc_delta, in);
        readVarUInt(last_doc_span, in);
        readVarUInt(seg_count, in);
        readVarUInt(seg_bytes, in);

        auto & meta = dir.segments[s];
        const UInt64 first_doc = prev_first_doc + first_doc_delta;
        const UInt64 last_doc = first_doc + last_doc_span;
        if (last_doc > std::numeric_limits<UInt32>::max() || seg_count == 0 || seg_count > dir.total_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index positions: invalid segment directory record");
        /// Segments never split a doc_id, so ranges must be disjoint and increasing.
        if (s > 0 && first_doc <= prev_last_doc)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: segment doc ranges overlap ({} <= {})", first_doc, prev_last_doc);

        meta.first_doc = static_cast<UInt32>(first_doc);
        meta.last_doc = static_cast<UInt32>(last_doc);
        meta.count = static_cast<UInt32>(seg_count);
        meta.bytes = seg_bytes;

        if (seg_bytes == 0 || seg_bytes > available_bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: segment payload size {} exceeds {} available bytes", seg_bytes, available_bytes);

        sum_count += seg_count;
        sum_bytes += seg_bytes;
        prev_first_doc = meta.first_doc;
        prev_last_doc = meta.last_doc;
    }

    if (sum_count != dir.total_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: segment counts add up to {} instead of {}", sum_count, dir.total_count);

    /// Resolve absolute payload offsets: they follow the directory back to back.
    const size_t directory_bytes = in.count() - count_before;
    checkFitsAvailable(directory_bytes + sum_bytes, available_bytes);

    UInt64 offset = blob_offset + directory_bytes;
    for (auto & meta : dir.segments)
    {
        meta.offset = offset;
        offset += meta.bytes;
    }

    return dir;
}

void TextIndexPositionCodec::decodeSegment(ReadBuffer & in, const SegmentMeta & meta, Encoding encoding, PositionList & pl, PaddedPODArray<char> & payload_scratch)
{
    if (encoding == Encoding::Pfor)
    {
        checkPforPayloadBounds(meta.bytes, meta.count);
        decodePforSegment(in, meta, pl, payload_scratch);
    }
    else
    {
        if (meta.bytes != meta.count * sizeof(RoaringishEntry))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: raw segment of {} entries has {} payload bytes", meta.count, meta.bytes);
        decodeRawSegment(in, meta, pl, payload_scratch);
    }
}

void TextIndexPositionCodec::decodeAllSegments(ReadBuffer & in, PODArray<RoaringishEntry> & entries, Encoding encoding, UInt64 position_cardinality, size_t available_bytes, DecodeScratch & scratch)
{
    /// Sequential read; blob_offset is only needed for seekable absolute offsets.
    auto dir = readSegmentDirectory(in, /*blob_offset=*/ 0, position_cardinality, available_bytes);

    entries.resize(dir.total_count);
    size_t written = 0;

    for (const auto & meta : dir.segments)
    {
        decodeSegment(in, meta, encoding, scratch.lanes, scratch.payload);
        for (size_t i = 0; i < scratch.lanes.size(); ++i)
            entries[written++] = RoaringishEntry{scratch.lanes.doc[i], scratch.lanes.group[i], scratch.lanes.bitmap[i]};
    }

    chassert(written == dir.total_count);
}

}
