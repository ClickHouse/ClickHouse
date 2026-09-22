#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <vector>
#include <gtest/gtest.h>

using namespace DB;

namespace
{

using Streams = std::map<String, String>;

DataTypePtr getMapType()
{
    return DataTypeFactory::instance().get("Map(String, UInt64)");
}

SerializationPtr getWithKeyColumnsSerialization(const DataTypePtr & type)
{
    SerializationInfoSettings info_settings;
    info_settings.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    return type->getSerialization(info_settings);
}

ColumnPtr makeColumn(const DataTypePtr & type)
{
    auto column = type->createColumn();
    column->insert(Map{Tuple{Field("b"), Field(UInt64(2))}, Tuple{Field("a"), Field(UInt64(1))}, Tuple{Field("a"), Field(UInt64(9))}});
    column->insert(Map{Tuple{Field("a"), Field(UInt64(3))}});
    column->insert(Map{});
    column->insert(Map{Tuple{Field("c"), Field(UInt64(4))}, Tuple{Field("a"), Field(UInt64(0))}});
    return std::move(column);
}

Streams serializeWithKeyColumns(const DataTypePtr & type, const IColumn & column)
{
    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        auto name = ISerialization::getFileNameForStream("m", path, {});
        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<WriteBufferFromOwnString>()).first;
        return it->second.get();
    };

    auto serialization = getWithKeyColumnsSerialization(type);
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, column.size(), settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);

    Streams streams;
    for (auto & [name, buffer] : buffers)
    {
        buffer->finalize();
        streams[name] = buffer->str();
    }
    return streams;
}

ColumnPtr deserializeWithKeyColumns(const DataTypePtr & type, const Streams & streams, size_t limit)
{
    std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        auto name = ISerialization::getFileNameForStream("m", path, {});
        auto stream_it = streams.find(name);
        if (stream_it == streams.end())
            return nullptr;

        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<ReadBufferFromString>(stream_it->second)).first;
        return it->second.get();
    };

    auto serialization = getWithKeyColumnsSerialization(type);
    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto column = type->createColumn();
    serialization->deserializeBinaryBulkWithMultipleStreams(*column, limit, settings, state, nullptr);
    return std::move(column);
}

Map sortedMap(const Field & field)
{
    auto map = field.safeGet<Map>();
    std::sort(map.begin(), map.end(), [](const Field & lhs, const Field & rhs)
    {
        return lhs.safeGet<Tuple>()[0] < rhs.safeGet<Tuple>()[0];
    });
    return map;
}

std::set<String> enumerateFileNames(const SerializationPtr & serialization, const ISerialization::SubstreamData & data)
{
    ISerialization::EnumerateStreamsSettings settings;
    std::set<String> names;
    serialization->enumerateStreams(
        settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            names.insert(ISerialization::getFileNameForStream("m", path, {}));
        },
        data);
    return names;
}

struct SubcolumnRead
{
    ColumnPtr column;
    std::vector<String> prefix_streams;
    std::vector<String> data_streams;
};

SubcolumnRead deserializeSubcolumn(const DataTypePtr & type, const Streams & streams, std::string_view subcolumn_name, size_t limit)
{
    auto serialization = getWithKeyColumnsSerialization(type);
    auto sub_serialization = type->getSubcolumnSerialization(subcolumn_name, serialization);
    auto sub_type = type->getSubcolumnType(subcolumn_name);
    auto sub = ISerialization::SubstreamData(sub_serialization).withType(sub_type);

    std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;
    std::vector<String> accessed;
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        auto name = ISerialization::getFileNameForStream("m", path, {});
        accessed.push_back(name);
        auto stream_it = streams.find(name);
        if (stream_it == streams.end())
            return nullptr;

        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<ReadBufferFromString>(stream_it->second)).first;
        return it->second.get();
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    sub.serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    SubcolumnRead result;
    result.prefix_streams = accessed;
    accessed.clear();

    auto column = sub.type->createColumn();
    sub.serialization->deserializeBinaryBulkWithMultipleStreams(*column, limit, settings, state, nullptr);
    result.column = std::move(column);
    result.data_streams = std::move(accessed);
    return result;
}

bool containsStream(const std::vector<String> & names, const String & needle)
{
    return std::find(names.begin(), names.end(), needle) != names.end();
}

}

TEST(MapWithKeyColumnsBulk, RoundTrip)
{
    auto type = getMapType();
    auto column = makeColumn(type);
    auto streams = serializeWithKeyColumns(type, *column);

    EXPECT_TRUE(streams.contains("m.keys_info"));
    EXPECT_TRUE(streams.contains("m.key_a"));
    EXPECT_TRUE(streams.contains("m.key_b"));
    EXPECT_TRUE(streams.contains("m.key_c"));
    EXPECT_TRUE(streams.contains("m.key_presence"));

    auto result = deserializeWithKeyColumns(type, streams, column->size());
    ASSERT_EQ(result->size(), column->size());

    /// Duplicate key `a` in row 0 keeps the first value; key order is manifest dictionary order.
    const std::vector<Map> expected = {
        Map{Tuple{Field("a"), Field(UInt64(1))}, Tuple{Field("b"), Field(UInt64(2))}},
        Map{Tuple{Field("a"), Field(UInt64(3))}},
        Map{},
        Map{Tuple{Field("a"), Field(UInt64(0))}, Tuple{Field("c"), Field(UInt64(4))}},
    };

    for (size_t row = 0; row < expected.size(); ++row)
        EXPECT_EQ(sortedMap((*result)[row]), expected[row]) << "row " << row;
}

TEST(MapWithKeyColumnsBulk, GranuleSplit)
{
    auto type = getMapType();
    auto column = makeColumn(type);
    auto serialization = getWithKeyColumnsSerialization(type);

    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;
    ISerialization::SerializeBinaryBulkSettings write_settings;
    write_settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        auto name = ISerialization::getFileNameForStream("m", path, {});
        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<WriteBufferFromOwnString>()).first;
        return it->second.get();
    };

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*column, write_settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, 2, write_settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 2, 2, write_settings, state);
    serialization->serializeBinaryBulkStateSuffix(write_settings, state);

    Streams streams;
    for (auto & [name, buffer] : buffers)
    {
        buffer->finalize();
        streams[name] = buffer->str();
    }

    auto first = deserializeWithKeyColumns(type, streams, 2);
    ASSERT_EQ(first->size(), 2u);
    EXPECT_EQ(sortedMap((*first)[0]), (Map{Tuple{Field("a"), Field(UInt64(1))}, Tuple{Field("b"), Field(UInt64(2))}}));
    EXPECT_EQ(sortedMap((*first)[1]), (Map{Tuple{Field("a"), Field(UInt64(3))}}));
}

TEST(MapWithKeyColumnsBulk, SingleKeyEnumeratesOnlyTargetStreams)
{
    auto type = getMapType();
    auto column = makeColumn(type);
    auto serialization = getWithKeyColumnsSerialization(type);
    auto parent = ISerialization::SubstreamData(serialization).withType(type).withColumn(column);

    const auto full = enumerateFileNames(serialization, parent);
    EXPECT_TRUE(full.contains("m.keys_info"));
    EXPECT_TRUE(full.contains("m.key_a"));
    EXPECT_TRUE(full.contains("m.key_b"));
    EXPECT_TRUE(full.contains("m.key_c"));
    EXPECT_TRUE(full.contains("m.key_presence"));

    auto key_a_ser = type->getSubcolumnSerialization("key_a", serialization);
    auto key_a_type = type->getSubcolumnType("key_a");
    auto key_a = ISerialization::SubstreamData(key_a_ser).withType(key_a_type);
    const auto key_a_files = enumerateFileNames(key_a.serialization, key_a);
    EXPECT_TRUE(key_a_files.contains("m.keys_info"));
    EXPECT_TRUE(key_a_files.contains("m.key_a"));
    EXPECT_FALSE(key_a_files.contains("m.key_b"));
    EXPECT_FALSE(key_a_files.contains("m.key_c"));
    EXPECT_FALSE(key_a_files.contains("m.key_presence"));

    auto exists_a_ser = type->getSubcolumnSerialization("exists_a", serialization);
    auto exists_a_type = type->getSubcolumnType("exists_a");
    auto exists_a = ISerialization::SubstreamData(exists_a_ser).withType(exists_a_type);
    const auto exists_a_files = enumerateFileNames(exists_a.serialization, exists_a);
    EXPECT_TRUE(exists_a_files.contains("m.keys_info"));
    EXPECT_TRUE(exists_a_files.contains("m.key_presence"));
    EXPECT_FALSE(exists_a_files.contains("m.key_a"));
    EXPECT_FALSE(exists_a_files.contains("m.key_b"));
    EXPECT_FALSE(exists_a_files.contains("m.key_c"));
}

TEST(MapWithKeyColumnsBulk, SingleKeyDeserializeDoesNotReadOtherKeys)
{
    auto type = getMapType();
    auto column = makeColumn(type);
    auto streams = serializeWithKeyColumns(type, *column);

    auto key_a = deserializeSubcolumn(type, streams, "key_a", column->size());
    ASSERT_EQ(key_a.column->size(), column->size());
    EXPECT_EQ((*key_a.column)[0], Field(UInt64(1)));
    EXPECT_EQ((*key_a.column)[1], Field(UInt64(3)));
    EXPECT_EQ((*key_a.column)[2], Field(UInt64(0)));
    EXPECT_EQ((*key_a.column)[3], Field(UInt64(0)));
    EXPECT_TRUE(containsStream(key_a.data_streams, "m.key_a"));
    EXPECT_FALSE(containsStream(key_a.data_streams, "m.key_b"));
    EXPECT_FALSE(containsStream(key_a.data_streams, "m.key_c"));
    EXPECT_FALSE(containsStream(key_a.data_streams, "m.key_presence"));

    auto missing = deserializeSubcolumn(type, streams, "key_missing", column->size());
    ASSERT_EQ(missing.column->size(), column->size());
    EXPECT_EQ((*missing.column)[0], Field(UInt64(0)));
    EXPECT_FALSE(containsStream(missing.data_streams, "m.key_a"));
    EXPECT_FALSE(containsStream(missing.data_streams, "m.key_b"));
    EXPECT_FALSE(containsStream(missing.data_streams, "m.key_c"));
    EXPECT_FALSE(containsStream(missing.data_streams, "m.key_presence"));

    auto exists_a = deserializeSubcolumn(type, streams, "exists_a", column->size());
    ASSERT_EQ(exists_a.column->size(), column->size());
    const auto & exists_data = assert_cast<const ColumnUInt8 &>(*exists_a.column).getData();
    ASSERT_EQ(exists_data.size(), 4u);
    EXPECT_EQ(exists_data[0], 1);
    EXPECT_EQ(exists_data[1], 1);
    EXPECT_EQ(exists_data[2], 0);
    EXPECT_EQ(exists_data[3], 1);
    EXPECT_TRUE(containsStream(exists_a.data_streams, "m.key_presence"));
    EXPECT_FALSE(containsStream(exists_a.data_streams, "m.key_a"));
    EXPECT_FALSE(containsStream(exists_a.data_streams, "m.key_b"));
    EXPECT_FALSE(containsStream(exists_a.data_streams, "m.key_c"));
}
