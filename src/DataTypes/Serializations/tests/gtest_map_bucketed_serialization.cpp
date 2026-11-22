#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

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

/// Reads `limit` rows out of the serialized streams, `chunk_size` rows per call (all of them in one call if 0).
/// `serialization` is either the whole-column serialization or a subcolumn one.
ColumnPtr deserializeRange(
    const SerializationPtr & serialization, const DataTypePtr & column_type, const Streams & streams, size_t limit, size_t chunk_size = 0)
{
    std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.read_statistics = true;
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
    size_t rows_per_call = chunk_size ? chunk_size : limit;
    for (size_t requested = 0; requested < limit; requested += rows_per_call)
        serialization->deserializeBinaryBulkWithMultipleStreams(*column, rows_per_call, settings, state, nullptr);
    return std::move(column);
}

void assertColumnsEqual(const IColumn & expected, const IColumn & actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t row = 0; row != actual.size(); ++row)
        ASSERT_EQ(expected[row], actual[row]) << "at row " << row;
}

/// The whole `Map` or one of its subcolumns: each one reassembles the buckets on its own.
struct ReadTarget
{
    String name;
    DataTypePtr type;
    SerializationPtr serialization;
};

std::vector<ReadTarget> getReadTargets(const DataTypePtr & type, const std::vector<String> & subcolumns)
{
    auto serialization = getBucketedSerialization(type);
    std::vector<ReadTarget> targets{{"map", type, serialization}};
    for (const auto & subcolumn : subcolumns)
        targets.push_back({subcolumn, type->getSubcolumnType(subcolumn), type->getSubcolumnSerialization(subcolumn, serialization)});
    return targets;
}

void expectReadError(
    const SerializationPtr & serialization, const DataTypePtr & column_type, const Streams & streams, size_t limit, int code)
{
    try
    {
        deserializeRange(serialization, column_type, streams, limit);
        FAIL() << "Expected an exception with code " << code;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), code) << e.message();
    }
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

/// Every bucket stores one size per row, so a bucket stream that ends before the others is a read error for every
/// reader of the buckets, with the `bucket_indexes` stream and without it (parts written before it existed).
TEST(MapBucketedSerialization, RejectsBucketsWithDifferentNumberOfRows)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    /// A shorter bucket 1 makes the readers index past its rows, a shorter bucket 0 makes them drop rows.
    for (size_t bucket = 0; bucket != 2; ++bucket)
    {
        SCOPED_TRACE("bucket " + std::to_string(bucket));
        auto damaged = streams;
        auto sizes_stream = std::to_string(bucket) + ".size0";
        ASSERT_TRUE(damaged.contains(sizes_stream));
        ASSERT_EQ(damaged[sizes_stream].size(), sizeof(UInt64) * num_rows);
        damaged[sizes_stream].resize(sizeof(UInt64) * num_rows / 2);

        ASSERT_TRUE(damaged.contains("bucket_indexes"));
        for (const auto & target : getReadTargets(type, {"keys", "values", "size0"}))
        {
            SCOPED_TRACE(target.name);
            expectReadError(target.serialization, target.type, damaged, num_rows, ErrorCodes::CANNOT_READ_ALL_DATA);
        }

        damaged.erase("bucket_indexes");
        for (const auto & target : getReadTargets(type, {"keys", "values"}))
        {
            SCOPED_TRACE(target.name + " without bucket_indexes");
            expectReadError(target.serialization, target.type, damaged, num_rows, ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
}

/// `bucket_indexes` holds one index per key-value pair of the buckets, so a shorter stream is a read error.
TEST(MapBucketedSerialization, RejectsTruncatedBucketIndexes)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    ASSERT_TRUE(streams.contains("bucket_indexes"));
    auto & indexes = streams["bucket_indexes"];
    indexes.resize(indexes.size() / 2);

    for (const auto & target : getReadTargets(type, {"keys", "values"}))
    {
        SCOPED_TRACE(target.name);
        expectReadError(target.serialization, target.type, streams, num_rows, ErrorCodes::CANNOT_READ_ALL_DATA);
    }
}

/// Indexes that are in range and as many as the key-value pairs, but refer to a bucket of a row more times than it
/// has pairs in that row, are a read error rather than a read of the pairs of the next row.
TEST(MapBucketedSerialization, RejectsBucketIndexesThatDoNotMatchBucketSizes)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    /// With 4 buckets an index is one byte.
    ASSERT_TRUE(streams.contains("bucket_indexes"));
    auto & indexes = streams["bucket_indexes"];
    const auto & offsets = assert_cast<const ColumnMap &>(*column).getNestedColumn().getOffsets();
    ASSERT_EQ(indexes.size(), offsets.back());

    /// Point all indexes of the first row whose pairs are in several buckets to the bucket of its first pair.
    bool found = false;
    for (size_t row = 0; row != num_rows && !found; ++row)
    {
        auto begin = indexes.begin() + offsets[ssize_t(row) - 1];
        auto end = indexes.begin() + offsets[ssize_t(row)];
        char first = *begin;
        if (std::any_of(begin, end, [first](char index) { return index != first; }))
        {
            std::fill(begin, end, first);
            found = true;
        }
    }
    ASSERT_TRUE(found);

    for (const auto & target : getReadTargets(type, {"keys", "values"}))
    {
        SCOPED_TRACE(target.name);
        expectReadError(target.serialization, target.type, streams, num_rows, ErrorCodes::INCORRECT_DATA);
    }
}

/// A valid read that stops at the end of the streams passes the checks: reading 20 rows 7 at a time ends with a call
/// that gets 6 rows from every stream.
TEST(MapBucketedSerialization, ReadsInChunks)
{
    auto type = getMapType();
    constexpr size_t num_rows = 20;
    auto column = makeTestColumn(type, num_rows);
    auto streams = serializeWithBuckets(type, *column);

    auto serialization = getBucketedSerialization(type);
    auto result = deserializeRange(serialization, type, streams, num_rows, 7);
    assertColumnsEqual(*column, *result);
}
