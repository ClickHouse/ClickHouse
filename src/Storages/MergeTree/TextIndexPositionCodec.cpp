#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

size_t TextIndexPositionCodec::encode(const std::vector<RoaringishEntry> & entries, WriteBuffer & out)
{
    size_t start_count = out.count();

    UInt64 count = entries.size();
    writeVarUInt(count, out);

    for (const auto & entry : entries)
    {
        UInt64 high = static_cast<UInt64>(entry.value >> 64);
        UInt64 low = static_cast<UInt64>(entry.value);
        writeBinaryLittleEndian(high, out);
        writeBinaryLittleEndian(low, out);
    }

    return out.count() - start_count;
}

void TextIndexPositionCodec::decode(ReadBuffer & in, std::vector<RoaringishEntry> & entries)
{
    UInt64 count = 0;
    readVarUInt(count, in);

    if (count == 0)
        return;

    entries.resize(count);

    for (size_t i = 0; i < count; ++i)
    {
        UInt64 high = 0;
        UInt64 low = 0;
        readBinaryLittleEndian(high, in);
        readBinaryLittleEndian(low, in);
        entries[i].value = (static_cast<UInt128>(high) << 64) | static_cast<UInt128>(low);
    }
}

}
