#include <gtest/gtest.h>
#include <config.h>
#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

#include <fmt/format.h>

using namespace DB;
using namespace DB::Parquet;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

struct Encoded
{
    String metadata;
    String value;
};

Encoded encodeField(const DataTypePtr & type, const Field & field)
{
    auto column = type->createColumn();
    column->insert(field);
    Encoded res;
    VariantValueEncoder encoder("v", VariantSettings{});
    encoder.encode(*column, *type, 0, res.metadata, res.value);
    return res;
}

/// Decodes into `Dynamic` and encodes the result again.
Encoded reencode(const Encoded & encoded)
{
    const DataTypePtr dynamic_type = std::make_shared<DataTypeDynamic>();
    auto column = dynamic_type->createColumn();
    VariantValueDecoder decoder(dynamic_type, "v", VariantSettings{});
    decoder.decode(encoded.metadata, encoded.value, *column);
    Encoded res;
    VariantValueEncoder encoder("v", VariantSettings{});
    encoder.encode(*column, *dynamic_type, 0, res.metadata, res.value);
    return res;
}

String hex(std::string_view bytes)
{
    String res;
    for (size_t i = 0; i < bytes.size(); ++i)
        res += fmt::format("{}{:02X}", i ? " " : "", static_cast<unsigned>(static_cast<unsigned char>(bytes[i])));
    return res;
}

void expectLayout(const Encoded & encoded, std::string_view metadata, std::string_view value)
{
    EXPECT_EQ(hex(encoded.metadata), metadata);
    EXPECT_EQ(hex(encoded.value), value);
    const Encoded again = reencode(encoded);
    EXPECT_EQ(hex(again.metadata), hex(encoded.metadata));
    EXPECT_EQ(hex(again.value), hex(encoded.value));
}

UInt32 readLittleEndian(std::string_view bytes, size_t pos, size_t size)
{
    UInt32 res = 0;
    for (size_t i = 0; i < size; ++i)
        res |= UInt32(static_cast<UInt8>(bytes[pos + i])) << (8 * i);
    return res;
}

}

TEST(ParquetVariantLayout, Primitives)
{
    /// Header: the primitive type id in the upper 6 bits, the basic type 0 (primitive) in the lower 2 bits.
    expectLayout(encodeField(std::make_shared<DataTypeInt64>(), Int64(5)), "01 00 00", "18 05 00 00 00 00 00 00 00");
    expectLayout(encodeField(std::make_shared<DataTypeInt8>(), Int64(-1)), "01 00 00", "0C FF");
    /// A short string: the length in the upper 6 bits, the basic type 1.
    expectLayout(encodeField(std::make_shared<DataTypeString>(), String("x")), "01 00 00", "05 78");
}

TEST(ParquetVariantLayout, LongString)
{
    const Encoded encoded = encodeField(std::make_shared<DataTypeString>(), String(64, 'a'));
    EXPECT_EQ(hex(encoded.value.substr(0, 5)), "40 40 00 00 00");
    EXPECT_EQ(encoded.value.size(), 5 + 64);
    EXPECT_EQ(hex(reencode(encoded).value), hex(encoded.value));
}

TEST(ParquetVariantLayout, SmallArray)
{
    const DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
    expectLayout(encodeField(type, Array{}), "01 00 00", "03 00 00");
    /// Header 03: basic type 3 (array), not large, 1-byte offsets; 2 elements; offsets 0, 2, 4.
    expectLayout(encodeField(type, Array{Int64(1), Int64(2)}), "01 00 00", "03 02 00 02 04 0C 01 0C 02");
}

TEST(ParquetVariantLayout, LargeArray)
{
    const DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
    Array elements;
    for (Int64 i = 0; i < 256; ++i)
        elements.push_back(i - 128);
    const Encoded encoded = encodeField(type, elements);

    /// Header 17: basic type 3, value header 5 = large (4-byte count) with 2-byte offsets, as the values take 512 bytes.
    EXPECT_EQ(hex(encoded.value.substr(0, 5)), "17 00 01 00 00");
    EXPECT_EQ(encoded.value.size(), 1 + 4 + 257 * 2 + 512);
    for (size_t i = 0; i <= 256; ++i)
        EXPECT_EQ(readLittleEndian(encoded.value, 5 + i * 2, 2), i * 2);
    EXPECT_EQ(hex(reencode(encoded).value), hex(encoded.value));
}

TEST(ParquetVariantLayout, ThreeByteOffsets)
{
    const DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    const Encoded encoded = encodeField(type, Array{String(70000, 'a')});

    /// Header 0B: basic type 3, value header 2 = not large with 3-byte offsets; offsets 0 and 70005 = 0x011175.
    EXPECT_EQ(hex(encoded.value.substr(0, 8)), "0B 01 00 00 00 75 11 01");
    /// The element is a long string of 70000 = 0x011170 bytes.
    EXPECT_EQ(hex(encoded.value.substr(8, 5)), "40 70 11 01 00");
    EXPECT_EQ(hex(reencode(encoded).value), hex(encoded.value));
}

TEST(ParquetVariantLayout, SmallObject)
{
    const DataTypePtr type = std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt8>(), std::make_shared<DataTypeString>()}, Strings{"b", "a"});
    /// The fields are sorted by key: "a" gets id 0 and its value comes first.
    /// Metadata: version 1 with 1-byte offsets; 2 keys; offsets 0, 1, 2; "a" "b".
    /// Value: header 02 = basic type 2 (object) with 1-byte ids and offsets; 2 fields; ids 0, 1; offsets 0, 2, 4.
    expectLayout(
        encodeField(type, Tuple{Int64(1), String("x")}),
        "01 02 00 01 02 61 62",
        "02 02 00 01 00 02 04 05 78 0C 01");
}

TEST(ParquetVariantLayout, NestedObjectSharesDictionary)
{
    const DataTypePtr inner = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt8>()}, Strings{"a"});
    const DataTypePtr type = std::make_shared<DataTypeTuple>(DataTypes{inner}, Strings{"b"});
    /// One dictionary for the whole value: "b" of the outer object comes first, then "a" of the nested one.
    expectLayout(
        encodeField(type, Tuple{Tuple{Int64(1)}}),
        "01 02 00 01 02 62 61",
        "02 01 00 00 07 02 01 01 00 02 0C 01");
}

TEST(ParquetVariantLayout, LargeObject)
{
    const DataTypePtr type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt8>());
    Map map;
    for (Int64 i = 0; i < 256; ++i)
        map.push_back(Tuple{fmt::format("k{:03}", i), i - 128});
    const Encoded encoded = encodeField(type, map);

    /// Metadata header 41: version 1 with 2-byte offsets, as the keys take 1024 bytes; 256 keys.
    EXPECT_EQ(hex(encoded.metadata.substr(0, 3)), "41 00 01");
    EXPECT_EQ(encoded.metadata.size(), 1 + 2 + 257 * 2 + 256 * 4);
    /// Value header 46: basic type 2, value header 0x11 = large (4-byte count) with 1-byte ids and 2-byte offsets.
    EXPECT_EQ(hex(encoded.value.substr(0, 5)), "46 00 01 00 00");
    EXPECT_EQ(encoded.value.size(), 1 + 4 + 256 + 257 * 2 + 512);
    for (size_t i = 0; i < 256; ++i)
        EXPECT_EQ(static_cast<unsigned char>(encoded.value[5 + i]), i);

    const Encoded again = reencode(encoded);
    EXPECT_EQ(hex(again.metadata), hex(encoded.metadata));
    EXPECT_EQ(hex(again.value), hex(encoded.value));
}

TEST(ParquetVariantLayout, TruncatedBlobsThrow)
{
    const DataTypePtr dynamic_type = std::make_shared<DataTypeDynamic>();
    VariantValueDecoder decoder(dynamic_type, "v", VariantSettings{});
    const String empty_metadata("\x01\x00\x00", 3);

    auto expect_incorrect_data = [&](std::string_view metadata, std::string_view value)
    {
        auto column = dynamic_type->createColumn();
        try
        {
            decoder.decode(metadata, value, *column);
            ADD_FAILURE() << "No exception for value " << hex(value);
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
        }
    };

    /// An int64 with 1 of 8 bytes.
    expect_incorrect_data(empty_metadata, std::string_view("\x18\x05", 2));
    /// An array of 2 elements with only one offset.
    expect_incorrect_data(empty_metadata, std::string_view("\x03\x02\x00", 3));
    /// An array whose only element, an int8, has no byte for its value.
    expect_incorrect_data(empty_metadata, std::string_view("\x03\x01\x00\x09\x0C", 5));
    /// An object that refers to the key id 0 with an empty dictionary.
    expect_incorrect_data(empty_metadata, std::string_view("\x02\x01\x00\x00\x02\x0C\x01", 7));
}

#endif
