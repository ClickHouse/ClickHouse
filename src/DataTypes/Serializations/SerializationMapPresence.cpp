#include <DataTypes/Serializations/SerializationMapPresence.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

size_t MapKeyPresenceBlock::kindArrayBytes(size_t key_count)
{
    return (key_count * 2 + 7) / 8;
}

size_t MapKeyPresenceBlock::bitmapBytes(size_t rows)
{
    return (rows + 7) / 8;
}

namespace
{

UInt8 computeKind(const std::vector<UInt8> & presence)
{
    if (presence.empty())
        return MapKeyPresenceBlock::KIND_ABSENT;

    bool saw_zero = false;
    bool saw_one = false;
    for (UInt8 bit : presence)
    {
        if (bit)
            saw_one = true;
        else
            saw_zero = true;
        if (saw_zero && saw_one)
            return MapKeyPresenceBlock::KIND_MIXED;
    }
    return saw_one ? MapKeyPresenceBlock::KIND_PRESENT : MapKeyPresenceBlock::KIND_ABSENT;
}

void writeKindArray(WriteBuffer & ostr, const std::vector<UInt8> & kinds)
{
    const size_t bytes = MapKeyPresenceBlock::kindArrayBytes(kinds.size());
    std::vector<UInt8> packed(bytes, 0);
    for (size_t i = 0; i < kinds.size(); ++i)
    {
        if (kinds[i] > MapKeyPresenceBlock::KIND_MIXED)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid presence kind {}", static_cast<UInt32>(kinds[i]));
        packed[i / 4] |= static_cast<UInt8>(kinds[i] << ((i % 4) * 2));
    }
    ostr.write(reinterpret_cast<const char *>(packed.data()), packed.size());
}

std::vector<UInt8> readKindArray(ReadBuffer & istr, size_t key_count)
{
    const size_t bytes = MapKeyPresenceBlock::kindArrayBytes(key_count);
    std::vector<UInt8> packed(bytes, 0);
    istr.readStrict(reinterpret_cast<char *>(packed.data()), bytes);

    std::vector<UInt8> kinds(key_count, 0);
    for (size_t i = 0; i < key_count; ++i)
    {
        kinds[i] = (packed[i / 4] >> ((i % 4) * 2)) & 0x3;
        if (kinds[i] > MapKeyPresenceBlock::KIND_MIXED)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid presence kind {} in Map presence block", static_cast<UInt32>(kinds[i]));
    }
    return kinds;
}

void writeBitmap(WriteBuffer & ostr, const std::vector<UInt8> & presence)
{
    const size_t bytes = MapKeyPresenceBlock::bitmapBytes(presence.size());
    std::vector<UInt8> packed(bytes, 0);
    for (size_t i = 0; i < presence.size(); ++i)
    {
        if (presence[i])
            packed[i / 8] |= static_cast<UInt8>(1u << (i % 8));
    }
    ostr.write(reinterpret_cast<const char *>(packed.data()), packed.size());
}

void readBitmap(ReadBuffer & istr, size_t rows, std::vector<UInt8> & out)
{
    const size_t bytes = MapKeyPresenceBlock::bitmapBytes(rows);
    std::vector<UInt8> packed(bytes, 0);
    istr.readStrict(reinterpret_cast<char *>(packed.data()), bytes);
    out.assign(rows, 0);
    for (size_t i = 0; i < rows; ++i)
        out[i] = (packed[i / 8] >> (i % 8)) & 1;
}

}

void MapKeyPresenceBlock::serialize(
    WriteBuffer & ostr,
    size_t rows,
    const std::vector<std::vector<UInt8>> & presence)
{
    std::vector<UInt8> kinds;
    kinds.reserve(presence.size());
    for (const auto & key_presence : presence)
    {
        if (key_presence.size() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Presence vector size {} does not match granule rows {}", key_presence.size(), rows);
        kinds.push_back(computeKind(key_presence));
    }

    writeKindArray(ostr, kinds);
    for (size_t i = 0; i < presence.size(); ++i)
    {
        if (kinds[i] == KIND_MIXED)
            writeBitmap(ostr, presence[i]);
    }
}

UInt8 MapKeyPresenceBlock::deserializeKind(ReadBuffer & istr, size_t key_count, size_t key_index)
{
    if (key_index >= key_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Presence key index {} is out of range for {} keys", key_index, key_count);

    auto kinds = readKindArray(istr, key_count);
    return kinds[key_index];
}

void MapKeyPresenceBlock::deserializeKey(
    ReadBuffer & istr,
    size_t rows,
    size_t key_count,
    size_t key_index,
    std::vector<UInt8> & out,
    size_t * bitmap_bytes_read)
{
    if (key_index >= key_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Presence key index {} is out of range for {} keys", key_index, key_count);

    auto kinds = readKindArray(istr, key_count);
    const UInt8 kind = kinds[key_index];
    if (kind != KIND_MIXED)
    {
        out.assign(rows, kind == KIND_PRESENT ? 1 : 0);
        return;
    }

    const size_t bytes = bitmapBytes(rows);
    size_t mixed_before = 0;
    for (size_t i = 0; i < key_index; ++i)
        mixed_before += kinds[i] == KIND_MIXED;

    istr.ignore(mixed_before * bytes);
    readBitmap(istr, rows, out);
    if (bitmap_bytes_read)
        *bitmap_bytes_read += bytes;
}

void MapKeyPresenceBlock::deserializeAll(
    ReadBuffer & istr,
    size_t rows,
    size_t key_count,
    std::vector<std::vector<UInt8>> & out,
    size_t * bitmap_bytes_read)
{
    auto kinds = readKindArray(istr, key_count);
    out.resize(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        if (kinds[i] != KIND_MIXED)
        {
            out[i].assign(rows, kinds[i] == KIND_PRESENT ? 1 : 0);
            continue;
        }
        readBitmap(istr, rows, out[i]);
        if (bitmap_bytes_read)
            *bitmap_bytes_read += bitmapBytes(rows);
    }
}

}
