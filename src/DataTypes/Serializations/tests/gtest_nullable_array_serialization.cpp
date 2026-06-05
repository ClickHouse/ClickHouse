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
