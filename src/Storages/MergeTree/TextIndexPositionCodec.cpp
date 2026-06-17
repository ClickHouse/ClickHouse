#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void TextIndexPositionCodec::encode(const std::vector<RoaringishEntry> & entries, WriteBuffer & out)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = entries.size();
    writeVarUInt(count, out);

    if (count > 0)
        out.write(reinterpret_cast<const char *>(entries.data()), count * sizeof(RoaringishEntry));
}

void TextIndexPositionCodec::decode(ReadBuffer & in, std::vector<RoaringishEntry> & entries)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = 0;
    readVarUInt(count, in);

    if (count == 0)
        return;

    entries.resize(count);
    in.readStrict(reinterpret_cast<char *>(entries.data()), count * sizeof(RoaringishEntry));
}

void TextIndexPositionCodec::decode(ReadBuffer & in, PositionList & pl)
{
    static_assert(sizeof(RoaringishEntry) == 12);

    UInt64 count = 0;
    readVarUInt(count, in);
    if (count == 0)
        return;

    pl.resize(count);
    /// On-disk layout is AoS (doc,group,bitmap per entry); de-interleave into the lanes.
    for (size_t i = 0; i < count; ++i)
    {
        RoaringishEntry e;
        in.readStrict(reinterpret_cast<char *>(&e), sizeof(e));
        pl.doc[i] = e.doc_id;
        pl.group[i] = e.group;
        pl.bitmap[i] = e.bitmap;
    }
}

}
