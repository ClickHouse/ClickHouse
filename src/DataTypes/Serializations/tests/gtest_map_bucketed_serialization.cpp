#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <map>
#include <memory>

using namespace DB;

namespace
{

constexpr size_t NUM_BUCKETS = 4;

/// A set of in-memory substreams, keyed by the substream name that a real data part would use
/// as a file name. It plays the role of the on-disk streams of a single column.
using Streams = std::map<String, String>;

DataTypePtr getMapType()
{
    return DataTypeFactory::instance().get("Map(String, UInt64)");
}

SerializationPtr getBucketedSerialization(const DataTypePtr & type)
{
    SerializationInfoSettings info_settings;
    info_settings.map_serialization_version = MergeTreeMapSerializationVersion::WITH_BUCKETS;
    return type->getSerialization(info_settings);
}

/// A `Map` column where every row has a different number of key-value pairs and the keys of a
/// single row land in several buckets in an order that is not the bucket order. Both properties
/// are needed to notice a bucket index stream that is out of sync with the bucket streams.
ColumnPtr makeTestColumn(const DataTypePtr & type, size_t num_rows)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != num_rows; ++row)
    {
        Map map;
        for (size_t i = 0; i != 1 + row % 7; ++i)
            map.push_back(Tuple{"key_" + std::to_string(row) + "_" + std::to_string(i), UInt64(row * 100 + i)});
        column->insert(map);
    }
    return std::move(column);
}

Streams serializeWithBuckets(const DataTypePtr & type, const IColumn & column)
{
    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX;
    settings.max_buckets_in_map = NUM_BUCKETS;
    settings.map_buckets_strategy = MergeTreeMapBucketsStrategy::CONSTANT;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        auto name = ISerialization::getSubcolumnNameForStream(path);
        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<WriteBufferFromOwnString>()).first;
        return it->second.get();
    };

    auto serialization = getBucketedSerialization(type);
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

/// Reads `limit` rows out of the serialized streams.
/// `serialization` is either the whole-column serialization or a subcolumn one.
ColumnPtr deserializeRange(
    const SerializationPtr & serialization, const DataTypePtr & column_type, const Streams & streams, size_t limit)
{
    std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        auto name = ISerialization::getSubcolumnNameForStream(path);
        auto stream_it = streams.find(name);
        if (stream_it == streams.end())
            return nullptr;

        auto it = buffers.find(name);
        if (it == buffers.end())
            it = buffers.emplace(name, std::make_unique<ReadBufferFromString>(stream_it->second)).first;
        return it->second.get();
    };
    settings.check_stream_exists_callback = [&](const ISerialization::SubstreamPath & path)
    {
        return streams.contains(ISerialization::getSubcolumnNameForStream(path));
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto column = column_type->createColumn();
    serialization->deserializeBinaryBulkWithMultipleStreams(*column, limit, settings, state, nullptr);
    return std::move(column);
}

void assertColumnsEqual(const IColumn & expected, const IColumn & actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t row = 0; row != actual.size(); ++row)
        ASSERT_EQ(expected[row], actual[row]) << "at row " << row;
}

}

/// A multi-bucket `Map` round-trips through the in-memory substreams: the `bucket_indexes` stream
/// restores the original key order across all buckets.
TEST(MapBucketedSerialization, RoundTrip)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    /// More than one bucket is what makes the bucket index stream necessary in the first place.
    ASSERT_TRUE(streams.contains("bucket_indexes"));

    auto serialization = getBucketedSerialization(type);
    auto result = deserializeRange(serialization, type, streams, num_rows);
    assertColumnsEqual(*column, *result);
}

/// The same holds for the `keys` subcolumn, which reassembles the keys of all buckets on its own.
TEST(MapBucketedSerialization, ReadKeysSubcolumn)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    auto keys_type = type->getSubcolumnType("keys");
    auto keys_serialization = type->getSubcolumnSerialization("keys", getBucketedSerialization(type));
    auto result = deserializeRange(keys_serialization, keys_type, streams, num_rows);

    /// Ground truth: the keys subcolumn extracted directly from the source column.
    auto expected_keys = type->getSubcolumn("keys", column);
    assertColumnsEqual(*expected_keys, *result);
}
