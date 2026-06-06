#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

using namespace DB;

namespace
{

using SubstreamPath = ISerialization::SubstreamPath;

ColumnPtr makeNestedArrayNullableArrayColumn()
{
    auto column = ColumnArray::create(
        ColumnNullable::create(
            ColumnArray::create(
                ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create()),
                ColumnArray::ColumnOffsets::create()),
            ColumnUInt8::create()),
        ColumnArray::ColumnOffsets::create());

    auto & array = assert_cast<ColumnArray &>(*column);

    /// [NULL]
    array.insert(Array{Field()});

    /// [[1, NULL, 3]]
    array.insert(Array{Array{UInt8(1), Field(), UInt8(3)}});

    /// [NULL, []]
    array.insert(Array{Field(), Array{}});

    return column;
}

}

static void testNestedArrayNullableArrayBulkRoundtrip(bool position_independent_encoding)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(Array(Nullable(UInt8))))");
    const auto column = makeNestedArrayNullableArrayColumn();

    std::unordered_map<String, std::unique_ptr<WriteBufferFromOwnString>> write_buffers;
    std::unordered_map<String, std::string> serialized;

    ISerialization::StreamFileNameSettings stream_settings;
    stream_settings.column_type = type.get();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.position_independent_encoding = position_independent_encoding;
    settings.getter = [&](const SubstreamPath & path) -> WriteBuffer *
    {
        const auto stream_name = ISerialization::getFileNameForStream("c", path, stream_settings);
        auto & buffer = write_buffers[stream_name];
        if (!buffer)
            buffer = std::make_unique<WriteBufferFromOwnString>();
        return buffer.get();
    };

    auto serialization = type->getDefaultSerialization();
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, column->size(), settings, state);

    for (auto & [name, buffer] : write_buffers)
        serialized[name] = buffer->str();

    EXPECT_TRUE(serialized.contains("c.size0"));
    EXPECT_TRUE(serialized.contains("c.array.null"));
    EXPECT_TRUE(serialized.contains("c.array.array.size0"));
    EXPECT_TRUE(serialized.contains("c.array.array.nested.null"));
    EXPECT_TRUE(serialized.contains("c.array.array.nested"));
    EXPECT_FALSE(serialized.contains("c.null"));

    std::unordered_map<String, std::string> read_data = serialized;

    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.position_independent_encoding = position_independent_encoding;
    deserialize_settings.getter = [&](const SubstreamPath & path) -> ReadBuffer *
    {
        const auto stream_name = ISerialization::getFileNameForStream("c", path, stream_settings);
        auto it = read_data.find(stream_name);
        if (it == read_data.end())
            return nullptr;
        return new ReadBufferFromString(it->second);
    };

    ColumnPtr restored = type->createColumn();
    ISerialization::DeserializeBinaryBulkStatePtr deserialize_state;
    serialization->deserializeBinaryBulkWithMultipleStreams(restored, 0, column->size(), deserialize_settings, deserialize_state, nullptr);
    EXPECT_EQ(column->dumpStructure(), restored->dumpStructure());
}

TEST(NullableArraySerialization, NestedArrayNullableArrayBulkRoundtrip)
{
    testNestedArrayNullableArrayBulkRoundtrip(false);
}

TEST(NullableArraySerialization, NestedArrayNullableArrayBulkRoundtripPositionIndependent)
{
    testNestedArrayNullableArrayBulkRoundtrip(true);
}

TEST(NullableArraySerialization, ArrayTupleNullableArrayStreamNamesAreUnique)
{
    auto type = DataTypeFactory::instance().get("Array(Tuple(a Nullable(Array(UInt8)), b Nullable(Array(UInt16))))");
    const auto column = type->createColumn();

    std::unordered_map<String, std::unique_ptr<WriteBufferFromOwnString>> write_buffers;
    std::unordered_set<String> stream_names;
    size_t stream_getter_calls = 0;

    ISerialization::StreamFileNameSettings stream_settings;
    stream_settings.column_type = type.get();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const SubstreamPath & path) -> WriteBuffer *
    {
        ++stream_getter_calls;

        const auto stream_name = ISerialization::getFileNameForStream("c", path, stream_settings);
        stream_names.insert(stream_name);

        auto & buffer = write_buffers[stream_name];
        if (!buffer)
            buffer = std::make_unique<WriteBufferFromOwnString>();
        return buffer.get();
    };

    auto serialization = type->getDefaultSerialization();
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, column->size(), settings, state);

    auto dump_stream_names = [&]
    {
        String result;
        for (const auto & stream_name : stream_names)
            result += stream_name + "\n";
        return result;
    };

    EXPECT_EQ(stream_getter_calls, stream_names.size());
    EXPECT_TRUE(stream_names.contains("c.size0"));
    EXPECT_TRUE(stream_names.contains("c.array%2Ea.null")) << dump_stream_names();
    EXPECT_TRUE(stream_names.contains("c.array%2Ea.array.size0")) << dump_stream_names();
    EXPECT_TRUE(stream_names.contains("c.array%2Ea.array.nested")) << dump_stream_names();
    EXPECT_TRUE(stream_names.contains("c.array%2Eb.null")) << dump_stream_names();
    EXPECT_TRUE(stream_names.contains("c.array%2Eb.array.size0")) << dump_stream_names();
    EXPECT_TRUE(stream_names.contains("c.array%2Eb.array.nested")) << dump_stream_names();
}

TEST(NullableArraySerialization, VariantNullableArrayAlternativesStreamNamesAreUnique)
{
    auto type = DataTypeFactory::instance().get("Variant(Array(Nullable(Array(UInt8))), Array(Nullable(Array(UInt16))))");

    std::unordered_set<String> stream_names;
    size_t stream_count = 0;

    ISerialization::StreamFileNameSettings stream_settings;
    stream_settings.column_type = type.get();

    auto serialization = type->getDefaultSerialization();
    serialization->enumerateStreams(
        [&](const SubstreamPath & path)
        {
            ++stream_count;
            stream_names.insert(ISerialization::getFileNameForStream("c", path, stream_settings));
        },
        type);

    auto dump_stream_names = [&]
    {
        String result;
        for (const auto & stream_name : stream_names)
            result += stream_name + "\n";
        return result;
    };

    bool has_uint8_alternative = false;
    bool has_uint16_alternative = false;
    for (const auto & stream_name : stream_names)
    {
        has_uint8_alternative |= stream_name.find("UInt8") != String::npos;
        has_uint16_alternative |= stream_name.find("UInt16") != String::npos;
    }

    EXPECT_EQ(stream_count, stream_names.size()) << dump_stream_names();
    EXPECT_TRUE(has_uint8_alternative) << dump_stream_names();
    EXPECT_TRUE(has_uint16_alternative) << dump_stream_names();
}
