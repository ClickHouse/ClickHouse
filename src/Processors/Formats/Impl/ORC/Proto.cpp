#include <Processors/Formats/Impl/ORC/Proto.h>

#include <Common/Exception.h>

#include <bit>
#include <cstring>
#include <exception>
#include <limits>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

namespace DB::ORC::Proto
{

void WireReader::check(size_t n) const
{
    if (static_cast<size_t>(end - pos) < n)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Truncated protobuf message in ORC metadata");
}

UInt64 WireReader::readVarint()
{
    UInt64 result = 0;
    for (size_t shift = 0; shift < 64; shift += 7)
    {
        check(1);
        UInt8 byte = static_cast<UInt8>(*pos++);
        result |= static_cast<UInt64>(byte & 0x7F) << shift;
        if (!(byte & 0x80))
            return result;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed varint in ORC metadata");
}

UInt64 WireReader::readFixed64()
{
    check(8);
    UInt64 v = 0;
    memcpy(&v, pos, 8);
    pos += 8;
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    return v;
}

UInt32 WireReader::readFixed32()
{
    check(4);
    UInt32 v = 0;
    memcpy(&v, pos, 4);
    pos += 4;
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    return v;
}

double WireReader::readDouble()
{
    return std::bit_cast<double>(readFixed64());
}

std::string_view WireReader::readBytes()
{
    UInt64 size = readVarint();
    check(size);
    std::string_view res(pos, size);
    pos += size;
    return res;
}

bool WireReader::nextField(UInt32 & field_number, WireType & wire_type)
{
    if (pos == end)
        return false;
    UInt64 key = readVarint();
    field_number = static_cast<UInt32>(key >> 3);
    wire_type = static_cast<WireType>(key & 7);
    if (field_number == 0 || (key >> 3) > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed field key in ORC metadata");
    return true;
}

void WireReader::skip(WireType wire_type)
{
    switch (wire_type)
    {
        case VARINT: readVarint(); return;
        case FIXED64: check(8); pos += 8; return;
        case LENGTH_DELIMITED: readBytes(); return;
        case FIXED32: check(4); pos += 4; return;
        case START_GROUP:
        case END_GROUP:
            break;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported protobuf wire type {} in ORC metadata", static_cast<int>(wire_type));
}

bool WireReader::readRepeatedVarint(WireType wire_type, std::vector<UInt64> & out)
{
    if (wire_type == LENGTH_DELIMITED)
    {
        WireReader packed(readBytes());
        while (packed.pos != packed.end)
            out.push_back(packed.readVarint());
        return true;
    }
    if (wire_type == VARINT)
    {
        out.push_back(readVarint());
        return true;
    }
    return false;
}


void WireWriter::appendRawVarint(std::string & s, UInt64 value)
{
    while (value >= 0x80)
    {
        s.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    s.push_back(static_cast<char>(value));
}

void WireWriter::writeVarint(UInt32 field, UInt64 value)
{
    writeKey(field, WireReader::VARINT);
    appendRawVarint(out, value);
}

void WireWriter::writeDouble(UInt32 field, double value)
{
    writeKey(field, WireReader::FIXED64);
    UInt64 v = std::bit_cast<UInt64>(value);
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    out.append(reinterpret_cast<const char *>(&v), 8);
}

void WireWriter::writeBytes(UInt32 field, std::string_view value)
{
    writeKey(field, WireReader::LENGTH_DELIMITED);
    appendRawVarint(out, value.size());
    out.append(value);
}

void WireWriter::writePackedVarint(UInt32 field, const std::vector<UInt64> & values)
{
    if (values.empty())
        return;
    std::string packed;
    for (UInt64 v : values)
        appendRawVarint(packed, v);
    writeBytes(field, packed);
}

void WireWriter::writePackedFixed64(UInt32 field, const std::vector<UInt64> & values)
{
    if (values.empty())
        return;
    std::string packed;
    for (UInt64 v : values)
    {
        if constexpr (std::endian::native == std::endian::big)
            v = std::byteswap(v);
        packed.append(reinterpret_cast<const char *>(&v), 8);
    }
    writeBytes(field, packed);
}


namespace
{

using WireType = WireReader::WireType;

/// A field with an unexpected wire type is an unknown field, which the Protocol Buffers rules
/// require to skip. It is detected before anything is read, and handled in `parseMessage`.
struct WireTypeMismatch : std::exception
{
};

void expectWireType(WireType actual, WireType expected)
{
    if (actual != expected)
        throw WireTypeMismatch{};
}

UInt64 readVarintField(WireReader & r, WireType t)
{
    expectWireType(t, WireReader::VARINT);
    return r.readVarint();
}

Int64 readSignedVarintField(WireReader & r, WireType t)
{
    expectWireType(t, WireReader::VARINT);
    return r.readSignedVarint();
}

UInt32 readUInt32Field(WireReader & r, WireType t)
{
    /// Protobuf truncates the 64-bit varint for 32-bit fields.
    return static_cast<UInt32>(readVarintField(r, t));
}

/// For `sint32` fields.
Int32 readSignedVarint32Field(WireReader & r, WireType t)
{
    expectWireType(t, WireReader::VARINT);
    UInt32 v = static_cast<UInt32>(r.readVarint());
    return static_cast<Int32>((v >> 1) ^ (~(v & 1) + 1));
}

double readDoubleField(WireReader & r, WireType t)
{
    expectWireType(t, WireReader::FIXED64);
    return r.readDouble();
}

std::string_view readBytesField(WireReader & r, WireType t)
{
    expectWireType(t, WireReader::LENGTH_DELIMITED);
    return r.readBytes();
}

template <typename F>
void parseMessage(std::string_view data, F && handle_field)
{
    WireReader r(data);
    UInt32 field = 0;
    WireType type = WireReader::VARINT;
    while (r.nextField(field, type))
    {
        bool handled = false;
        try
        {
            handled = handle_field(r, field, type);
        }
        catch (const WireTypeMismatch &)
        {
            handled = false;
        }
        if (!handled)
            r.skip(type);
    }
}

IntegerStatistics parseIntegerStatistics(std::string_view data)
{
    IntegerStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = readSignedVarintField(r, type); return true;
            case 2: res.maximum = readSignedVarintField(r, type); return true;
            case 3: res.sum = readSignedVarintField(r, type); return true;
            default: return false;
        }
    });
    return res;
}

DoubleStatistics parseDoubleStatistics(std::string_view data)
{
    DoubleStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = readDoubleField(r, type); return true;
            case 2: res.maximum = readDoubleField(r, type); return true;
            case 3: res.sum = readDoubleField(r, type); return true;
            default: return false;
        }
    });
    return res;
}

StringStatistics parseStringStatistics(std::string_view data)
{
    StringStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = std::string(readBytesField(r, type)); return true;
            case 2: res.maximum = std::string(readBytesField(r, type)); return true;
            case 3: res.sum = readSignedVarintField(r, type); return true;
            case 4: res.lower_bound = std::string(readBytesField(r, type)); return true;
            case 5: res.upper_bound = std::string(readBytesField(r, type)); return true;
            default: return false;
        }
    });
    return res;
}

BucketStatistics parseBucketStatistics(std::string_view data)
{
    BucketStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        if (field != 1)
            return false;
        return r.readRepeatedVarint(type, res.count);
    });
    return res;
}

DecimalStatistics parseDecimalStatistics(std::string_view data)
{
    DecimalStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = std::string(readBytesField(r, type)); return true;
            case 2: res.maximum = std::string(readBytesField(r, type)); return true;
            case 3: res.sum = std::string(readBytesField(r, type)); return true;
            default: return false;
        }
    });
    return res;
}

DateStatistics parseDateStatistics(std::string_view data)
{
    DateStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = readSignedVarint32Field(r, type); return true;
            case 2: res.maximum = readSignedVarint32Field(r, type); return true;
            default: return false;
        }
    });
    return res;
}

TimestampStatistics parseTimestampStatistics(std::string_view data)
{
    TimestampStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.minimum = readSignedVarintField(r, type); return true;
            case 2: res.maximum = readSignedVarintField(r, type); return true;
            case 3: res.minimum_utc = readSignedVarintField(r, type); return true;
            case 4: res.maximum_utc = readSignedVarintField(r, type); return true;
            case 5: res.minimum_nanos = static_cast<Int32>(readVarintField(r, type)); return true;
            case 6: res.maximum_nanos = static_cast<Int32>(readVarintField(r, type)); return true;
            default: return false;
        }
    });
    return res;
}

BinaryStatistics parseBinaryStatistics(std::string_view data)
{
    BinaryStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        if (field != 1)
            return false;
        res.sum = readSignedVarintField(r, type);
        return true;
    });
    return res;
}

CollectionStatistics parseCollectionStatistics(std::string_view data)
{
    CollectionStatistics res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.min_children = readVarintField(r, type); return true;
            case 2: res.max_children = readVarintField(r, type); return true;
            case 3: res.total_children = readVarintField(r, type); return true;
            default: return false;
        }
    });
    return res;
}

Stream parseStream(std::string_view data)
{
    Stream res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.kind = readUInt32Field(r, type); return true;
            case 2: res.column = readUInt32Field(r, type); return true;
            case 3: res.length = readVarintField(r, type); return true;
            default: return false;
        }
    });
    return res;
}

ColumnEncoding parseColumnEncoding(std::string_view data)
{
    ColumnEncoding res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1:
            {
                UInt32 kind = readUInt32Field(r, type);
                if (kind > static_cast<UInt32>(ColumnEncodingKind::DICTIONARY_V2))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ORC column encoding kind {}", kind);
                res.kind = static_cast<ColumnEncodingKind>(kind);
                return true;
            }
            case 2: res.dictionary_size = readUInt32Field(r, type); return true;
            case 3: res.bloom_encoding = readUInt32Field(r, type); return true;
            default: return false;
        }
    });
    return res;
}

Type parseType(std::string_view data)
{
    Type res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.kind = readUInt32Field(r, type); return true;
            case 2: return r.readRepeatedVarint(type, res.subtypes);
            case 3: res.field_names.emplace_back(readBytesField(r, type)); return true;
            case 4: res.maximum_length = readUInt32Field(r, type); return true;
            case 5: res.precision = readUInt32Field(r, type); return true;
            case 6: res.scale = readUInt32Field(r, type); return true;
            case 7:
            {
                StringPair pair;
                parseMessage(readBytesField(r, type), [&](WireReader & rr, UInt32 f, WireType t)
                {
                    switch (f)
                    {
                        case 1: pair.key = std::string(readBytesField(rr, t)); return true;
                        case 2: pair.value = std::string(readBytesField(rr, t)); return true;
                        default: return false;
                    }
                });
                res.attributes.push_back(std::move(pair));
                return true;
            }
            case 8: res.crs = std::string(readBytesField(r, type)); return true;
            case 9: res.algorithm = readUInt32Field(r, type); return true;
            default: return false;
        }
    });
    return res;
}

StripeInformation parseStripeInformation(std::string_view data)
{
    StripeInformation res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.offset = readVarintField(r, type); return true;
            case 2: res.index_length = readVarintField(r, type); return true;
            case 3: res.data_length = readVarintField(r, type); return true;
            case 4: res.footer_length = readVarintField(r, type); return true;
            case 5: res.number_of_rows = readVarintField(r, type); return true;
            default: return false;
        }
    });
    return res;
}

UserMetadataItem parseUserMetadataItem(std::string_view data)
{
    UserMetadataItem res;
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: res.name = std::string(readBytesField(r, type)); return true;
            case 2: res.value = std::string(readBytesField(r, type)); return true;
            default: return false;
        }
    });
    return res;
}

std::string serializeIntegerStatistics(const IntegerStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.minimum) w.writeSignedVarint(1, *s.minimum);
    if (s.maximum) w.writeSignedVarint(2, *s.maximum);
    if (s.sum) w.writeSignedVarint(3, *s.sum);
    return out;
}

std::string serializeDoubleStatistics(const DoubleStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.minimum) w.writeDouble(1, *s.minimum);
    if (s.maximum) w.writeDouble(2, *s.maximum);
    if (s.sum) w.writeDouble(3, *s.sum);
    return out;
}

std::string serializeStringStatistics(const StringStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.minimum) w.writeBytes(1, *s.minimum);
    if (s.maximum) w.writeBytes(2, *s.maximum);
    if (s.sum) w.writeSignedVarint(3, *s.sum);
    if (s.lower_bound) w.writeBytes(4, *s.lower_bound);
    if (s.upper_bound) w.writeBytes(5, *s.upper_bound);
    return out;
}

std::string serializeDecimalStatistics(const DecimalStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.minimum) w.writeBytes(1, *s.minimum);
    if (s.maximum) w.writeBytes(2, *s.maximum);
    if (s.sum) w.writeBytes(3, *s.sum);
    return out;
}

std::string serializeDateStatistics(const DateStatistics & s)
{
    std::string out;
    WireWriter w(out);
    auto zigzag32 = [](Int32 v) { return static_cast<UInt64>((static_cast<UInt32>(v) << 1) ^ static_cast<UInt32>(v >> 31)); };
    if (s.minimum) w.writeVarint(1, zigzag32(*s.minimum));
    if (s.maximum) w.writeVarint(2, zigzag32(*s.maximum));
    return out;
}

std::string serializeTimestampStatistics(const TimestampStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.minimum) w.writeSignedVarint(1, *s.minimum);
    if (s.maximum) w.writeSignedVarint(2, *s.maximum);
    if (s.minimum_utc) w.writeSignedVarint(3, *s.minimum_utc);
    if (s.maximum_utc) w.writeSignedVarint(4, *s.maximum_utc);
    /// `int32` fields: negative values are sign-extended to 64 bits.
    if (s.minimum_nanos) w.writeVarint(5, static_cast<UInt64>(static_cast<Int64>(*s.minimum_nanos)));
    if (s.maximum_nanos) w.writeVarint(6, static_cast<UInt64>(static_cast<Int64>(*s.maximum_nanos)));
    return out;
}

std::string serializeCollectionStatistics(const CollectionStatistics & s)
{
    std::string out;
    WireWriter w(out);
    if (s.min_children) w.writeVarint(1, *s.min_children);
    if (s.max_children) w.writeVarint(2, *s.max_children);
    if (s.total_children) w.writeVarint(3, *s.total_children);
    return out;
}

std::string serializeType(const Type & t)
{
    std::string out;
    WireWriter w(out);
    w.writeVarint(1, t.kind);
    w.writePackedVarint(2, t.subtypes);
    for (const auto & name : t.field_names)
        w.writeBytes(3, name);
    if (t.maximum_length) w.writeVarint(4, *t.maximum_length);
    if (t.precision) w.writeVarint(5, *t.precision);
    if (t.scale) w.writeVarint(6, *t.scale);
    for (const auto & attr : t.attributes)
    {
        std::string pair;
        WireWriter pw(pair);
        pw.writeBytes(1, attr.key);
        pw.writeBytes(2, attr.value);
        w.writeBytes(7, pair);
    }
    if (t.crs) w.writeBytes(8, *t.crs);
    if (t.algorithm) w.writeVarint(9, *t.algorithm);
    return out;
}

std::string serializeStripeInformation(const StripeInformation & s)
{
    std::string out;
    WireWriter w(out);
    w.writeVarint(1, s.offset);
    w.writeVarint(2, s.index_length);
    w.writeVarint(3, s.data_length);
    w.writeVarint(4, s.footer_length);
    w.writeVarint(5, s.number_of_rows);
    return out;
}

}


void PostScript::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: footer_length = readVarintField(r, type); return true;
            case 2:
            {
                UInt64 kind = readVarintField(r, type);
                if (kind > static_cast<UInt64>(CompressionKind::BROTLI))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ORC compression kind {}", kind);
                compression = static_cast<CompressionKind>(kind);
                return true;
            }
            case 3: compression_block_size = readVarintField(r, type); return true;
            case 4: return r.readRepeatedVarint(type, version);
            case 5: metadata_length = readVarintField(r, type); return true;
            case 6: writer_version = readUInt32Field(r, type); return true;
            case 8000: magic = std::string(readBytesField(r, type)); return true;
            default: return false;
        }
    });
}

std::string PostScript::serialize() const
{
    std::string out;
    WireWriter w(out);
    w.writeVarint(1, footer_length);
    w.writeVarint(2, static_cast<UInt64>(compression));
    w.writeVarint(3, compression_block_size);
    w.writePackedVarint(4, version);
    w.writeVarint(5, metadata_length);
    w.writeVarint(6, writer_version);
    w.writeBytes(8000, magic);
    return out;
}

void ColumnStatistics::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: number_of_values = readVarintField(r, type); return true;
            case 2: int_statistics = parseIntegerStatistics(readBytesField(r, type)); return true;
            case 3: double_statistics = parseDoubleStatistics(readBytesField(r, type)); return true;
            case 4: string_statistics = parseStringStatistics(readBytesField(r, type)); return true;
            case 5: bucket_statistics = parseBucketStatistics(readBytesField(r, type)); return true;
            case 6: decimal_statistics = parseDecimalStatistics(readBytesField(r, type)); return true;
            case 7: date_statistics = parseDateStatistics(readBytesField(r, type)); return true;
            case 8: binary_statistics = parseBinaryStatistics(readBytesField(r, type)); return true;
            case 9: timestamp_statistics = parseTimestampStatistics(readBytesField(r, type)); return true;
            case 10: has_null = readVarintField(r, type) != 0; return true;
            case 11: bytes_on_disk = readVarintField(r, type); return true;
            case 12: collection_statistics = parseCollectionStatistics(readBytesField(r, type)); return true;
            default: return false;
        }
    });
}

std::string ColumnStatistics::serialize() const
{
    std::string out;
    WireWriter w(out);
    if (number_of_values) w.writeVarint(1, *number_of_values);
    if (int_statistics) w.writeBytes(2, serializeIntegerStatistics(*int_statistics));
    if (double_statistics) w.writeBytes(3, serializeDoubleStatistics(*double_statistics));
    if (string_statistics) w.writeBytes(4, serializeStringStatistics(*string_statistics));
    if (bucket_statistics)
    {
        std::string bucket;
        WireWriter(bucket).writePackedVarint(1, bucket_statistics->count);
        w.writeBytes(5, bucket);
    }
    if (decimal_statistics) w.writeBytes(6, serializeDecimalStatistics(*decimal_statistics));
    if (date_statistics) w.writeBytes(7, serializeDateStatistics(*date_statistics));
    if (binary_statistics)
    {
        std::string binary;
        if (binary_statistics->sum)
            WireWriter(binary).writeSignedVarint(1, *binary_statistics->sum);
        w.writeBytes(8, binary);
    }
    if (timestamp_statistics) w.writeBytes(9, serializeTimestampStatistics(*timestamp_statistics));
    if (has_null) w.writeVarint(10, *has_null);
    if (bytes_on_disk) w.writeVarint(11, *bytes_on_disk);
    if (collection_statistics) w.writeBytes(12, serializeCollectionStatistics(*collection_statistics));
    return out;
}

void RowIndex::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        if (field != 1)
            return false;
        RowIndexEntry e;
        parseMessage(readBytesField(r, type), [&](WireReader & rr, UInt32 f, WireType t)
        {
            switch (f)
            {
                case 1: return rr.readRepeatedVarint(t, e.positions);
                case 2: e.statistics.emplace().parse(readBytesField(rr, t)); return true;
                default: return false;
            }
        });
        entry.push_back(std::move(e));
        return true;
    });
}

std::string RowIndex::serialize() const
{
    std::string out;
    WireWriter w(out);
    for (const auto & e : entry)
    {
        std::string entry_data;
        WireWriter ew(entry_data);
        ew.writePackedVarint(1, e.positions);
        if (e.statistics)
            ew.writeBytes(2, e.statistics->serialize());
        w.writeBytes(1, entry_data);
    }
    return out;
}

void BloomFilterIndex::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        if (field != 1)
            return false;
        BloomFilter bf;
        parseMessage(readBytesField(r, type), [&](WireReader & rr, UInt32 f, WireType t)
        {
            switch (f)
            {
                case 1: bf.num_hash_functions = readUInt32Field(rr, t); return true;
                case 2:
                {
                    if (t == WireReader::LENGTH_DELIMITED)
                    {
                        /// Packed fixed64: a sequence of 8-byte little-endian values.
                        std::string_view bytes = rr.readBytes();
                        if (bytes.size() % 8 != 0)
                            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed bloom filter bitset in ORC file");
                        WireReader packed(bytes);
                        for (size_t i = 0; i < bytes.size() / 8; ++i)
                            bf.bitset.push_back(packed.readFixed64());
                    }
                    else
                    {
                        expectWireType(t, WireReader::FIXED64);
                        bf.bitset.push_back(rr.readFixed64());
                    }
                    return true;
                }
                case 3: bf.utf8bitset = std::string(readBytesField(rr, t)); return true;
                default: return false;
            }
        });
        bloom_filter.push_back(std::move(bf));
        return true;
    });
}

void StripeFooter::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: streams.push_back(parseStream(readBytesField(r, type))); return true;
            case 2: columns.push_back(parseColumnEncoding(readBytesField(r, type))); return true;
            case 3: writer_timezone = std::string(readBytesField(r, type)); return true;
            case 4: readBytesField(r, type); ++encryption_variants; return true;
            default: return false;
        }
    });
}

std::string StripeFooter::serialize() const
{
    std::string out;
    WireWriter w(out);
    for (const auto & s : streams)
    {
        std::string stream;
        WireWriter sw(stream);
        if (s.kind)
            sw.writeVarint(1, *s.kind);
        sw.writeVarint(2, s.column);
        sw.writeVarint(3, s.length);
        w.writeBytes(1, stream);
    }
    for (const auto & c : columns)
    {
        std::string encoding;
        WireWriter ew(encoding);
        ew.writeVarint(1, static_cast<UInt64>(c.kind));
        if (c.kind == ColumnEncodingKind::DICTIONARY || c.kind == ColumnEncodingKind::DICTIONARY_V2)
            ew.writeVarint(2, c.dictionary_size);
        w.writeBytes(2, encoding);
    }
    if (writer_timezone)
        w.writeBytes(3, *writer_timezone);
    return out;
}

void Metadata::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        if (field != 1)
            return false;
        StripeStatistics stats;
        parseMessage(readBytesField(r, type), [&](WireReader & rr, UInt32 f, WireType t)
        {
            if (f != 1)
                return false;
            stats.col_stats.emplace_back().parse(readBytesField(rr, t));
            return true;
        });
        stripe_stats.push_back(std::move(stats));
        return true;
    });
}

std::string Metadata::serialize() const
{
    std::string out;
    WireWriter w(out);
    for (const auto & stats : stripe_stats)
    {
        std::string stripe;
        WireWriter sw(stripe);
        for (const auto & col : stats.col_stats)
            sw.writeBytes(1, col.serialize());
        w.writeBytes(1, stripe);
    }
    return out;
}

void Footer::parse(std::string_view data)
{
    parseMessage(data, [&](WireReader & r, UInt32 field, WireType type)
    {
        switch (field)
        {
            case 1: header_length = readVarintField(r, type); return true;
            case 2: content_length = readVarintField(r, type); return true;
            case 3: stripes.push_back(parseStripeInformation(readBytesField(r, type))); return true;
            case 4: types.push_back(parseType(readBytesField(r, type))); return true;
            case 5: metadata.push_back(parseUserMetadataItem(readBytesField(r, type))); return true;
            case 6: number_of_rows = readVarintField(r, type); return true;
            case 7: statistics.emplace_back().parse(readBytesField(r, type)); return true;
            case 8: row_index_stride = readUInt32Field(r, type); return true;
            case 9: writer = readUInt32Field(r, type); return true;
            case 10: readBytesField(r, type); has_encryption = true; return true;
            case 12: software_version = std::string(readBytesField(r, type)); return true;
            default: return false;
        }
    });
}

std::string Footer::serialize() const
{
    std::string out;
    WireWriter w(out);
    w.writeVarint(1, header_length);
    w.writeVarint(2, content_length);
    for (const auto & s : stripes)
        w.writeBytes(3, serializeStripeInformation(s));
    for (const auto & t : types)
        w.writeBytes(4, serializeType(t));
    for (const auto & item : metadata)
    {
        std::string m;
        WireWriter mw(m);
        mw.writeBytes(1, item.name);
        mw.writeBytes(2, item.value);
        w.writeBytes(5, m);
    }
    w.writeVarint(6, number_of_rows);
    for (const auto & s : statistics)
        w.writeBytes(7, s.serialize());
    w.writeVarint(8, row_index_stride);
    if (writer)
        w.writeVarint(9, *writer);
    if (software_version)
        w.writeBytes(12, *software_version);
    return out;
}

}
