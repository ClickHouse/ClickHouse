#include <Columns/ColumnMap.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

using namespace DB;

namespace
{

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
    column->insert(Map{Tuple{Field("b"), Field(UInt64(2))}, Tuple{Field("a"), Field(UInt64(1))}});
    column->insert(Map{Tuple{Field("a"), Field(UInt64(3))}, Tuple{Field("c"), Field(UInt64(4))}});
    return std::move(column);
}

String serializeManifest(const SerializationPtr & serialization, const IColumn & column)
{
    WriteBufferFromOwnString buffer;
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        EXPECT_EQ(path.back().type, ISerialization::Substream::MapKeysInfo);
        return &buffer;
    };

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    buffer.finalize();
    return buffer.str();
}

MapKeyManifest deserializeManifest(const SerializationPtr & serialization, const String & bytes)
{
    ReadBufferFromString buffer(bytes);
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        EXPECT_EQ(path.back().type, ISerialization::Substream::MapKeysInfo);
        return &buffer;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    return assert_cast<const SerializationMapWithKeyColumns &>(*serialization).getManifestFromState(state);
}

}

TEST(MapWithKeyColumnsManifest, RoundTrip)
{
    auto type = getMapType();
    auto serialization = getWithKeyColumnsSerialization(type);
    auto column = makeColumn(type);

    auto manifest = deserializeManifest(serialization, serializeManifest(serialization, *column));
    ASSERT_EQ(manifest.keys.size(), 3u);
    EXPECT_EQ(manifest.keys[0].key, Field("a"));
    EXPECT_EQ(manifest.keys[1].key, Field("b"));
    EXPECT_EQ(manifest.keys[2].key, Field("c"));
    for (const auto & entry : manifest.keys)
    {
        EXPECT_EQ(entry.presence_kind, MapKeyPresenceKind::Tracked);
        EXPECT_EQ(entry.value_kind, MapKeyValueKind::Dense);
    }
}

TEST(MapWithKeyColumnsManifest, TruncatedThrowsIncorrectData)
{
    auto type = getMapType();
    auto serialization = getWithKeyColumnsSerialization(type);
    auto column = makeColumn(type);
    auto bytes = serializeManifest(serialization, *column);
    ASSERT_GT(bytes.size(), 1u);

    for (size_t cut : {size_t(0), size_t(1), bytes.size() / 2, bytes.size() - 1})
    {
        EXPECT_THROW(
            {
                deserializeManifest(serialization, bytes.substr(0, cut));
            },
            Exception);
    }

    try
    {
        deserializeManifest(serialization, bytes.substr(0, bytes.size() / 2));
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(MapWithKeyColumnsManifest, EnumerateStreamsFromColumn)
{
    auto type = getMapType();
    auto serialization = getWithKeyColumnsSerialization(type);
    auto column = makeColumn(type);

    std::vector<String> names;
    ISerialization::EnumerateStreamsSettings settings;
    serialization->enumerateStreams(
        settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            names.push_back(ISerialization::getSubcolumnNameForStream(path));
        },
        ISerialization::SubstreamData(serialization).withType(type).withColumn(column));

    ASSERT_FALSE(names.empty());
    EXPECT_EQ(names.front(), "keys_info");
    EXPECT_TRUE(std::find(names.begin(), names.end(), "key_a") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "key_b") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "key_c") != names.end());
    EXPECT_EQ(std::count(names.begin(), names.end(), "exists_"), 1);
}
