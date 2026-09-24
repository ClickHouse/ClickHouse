#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/transformEndianness.h>

#include <bit>

namespace DB
{

namespace
{
/// Entries are little-endian on disk; swap each lane to/from native (compiles away on little-endian hosts).
[[maybe_unused]] void transformEntryEndianness(RoaringishEntry & e)
{
    transformEndianness<std::endian::little>(e.doc_id);
    transformEndianness<std::endian::little>(e.group);
    transformEndianness<std::endian::little>(e.bitmap);
}
}

void TextIndexPositionCodec::encode(std::span<const RoaringishEntry> entries, WriteBuffer & out)
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

void TextIndexPositionCodec::decode(ReadBuffer & in, PODArray<RoaringishEntry> & entries)
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

void TextIndexPositionCodec::decode(ReadBuffer & in, PositionList & pl)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = 0;
    readVarUInt(count, in);
    if (count == 0)
        return;

    pl.resize(count);
    /// On-disk layout is AoS (doc, group, bitmap per entry); de-interleave into the lanes.
    for (size_t i = 0; i < count; ++i)
    {
        RoaringishEntry entry{};
        in.readStrict(reinterpret_cast<char *>(&entry), sizeof(entry));
        if constexpr (std::endian::native != std::endian::little)
            transformEntryEndianness(entry);
        pl.doc[i] = entry.doc_id;
        pl.group[i] = entry.group;
        pl.bitmap[i] = entry.bitmap;
    }
}

}
