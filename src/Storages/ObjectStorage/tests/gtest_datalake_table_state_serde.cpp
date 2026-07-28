#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.h>
#include <Core/ProtocolDefines.h>
#include "config.h"

using namespace DB;

namespace
{
std::vector<Iceberg::TableStateSnapshot> example_iceberg_states = {
    Iceberg::TableStateSnapshot{
        .metadata_file_path = "abacaba",
        .metadata_version = std::numeric_limits<Int32>::min(),
        .schema_id = std::numeric_limits<Int32>::max(),
        .snapshot_id = std::optional{std::numeric_limits<Int64>::max()},
    },
    Iceberg::TableStateSnapshot{
        .metadata_file_path = "",
        .metadata_version = 0,
        .schema_id = std::numeric_limits<Int32>::max(),
        .snapshot_id = std::nullopt,
    },
};
}

TEST(DatalakeStateSerde, IcebergStateSerde)
{

    for (const auto & iceberg_state : example_iceberg_states)
    {
        String str;
        {
            WriteBufferFromString write_buffer{str};
            iceberg_state.serialize(write_buffer);
        }
        ReadBufferFromMemory read_buffer(str) ;
        Iceberg::TableStateSnapshot serde_state = Iceberg::TableStateSnapshot::deserialize(read_buffer, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);
        ASSERT_TRUE(read_buffer.eof());
        ASSERT_EQ(iceberg_state, serde_state);
    }
}


TEST(DatalakeStateSerde, DataLakeStateSerde)
{

    for (const auto & iceberg_state : example_iceberg_states)
    {
        DataLakeTableStateSnapshot initial_state = iceberg_state;
        String str;
        {
            WriteBufferFromString write_buffer{str};
            serializeDataLakeTableStateSnapshot(initial_state, write_buffer);
        }
        ReadBufferFromMemory read_buffer(str);
        DataLakeTableStateSnapshot serde_state = deserializeDataLakeTableStateSnapshot(read_buffer);
        ASSERT_TRUE(read_buffer.eof());
        ASSERT_EQ(initial_state, serde_state);
    }
}


TEST(DatalakeStateSerde, IcebergObjectSerializableInfoRoundTrip)
{
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = DB::Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/path/to/file.parquet");
    info.underlying_format_read_schema_id = 42;
    info.schema_id_relevant_to_iterator = 7;
    info.sequence_number = 123456;
    info.file_format = "PARQUET";
    info.position_deletes_objects = {{"s3://bucket/deletes/pos1.parquet", "PARQUET", "s3://bucket/path/to/file.parquet"}};
    info.equality_deletes_objects = {{"s3://bucket/deletes/eq1.parquet", "PARQUET", std::vector<Int32>{1, 2, 3}, 42}};
    info.record_count = 100500;
    info.file_size_in_bytes = 999888777;

    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.serializeForClusterFunctionProtocol(write_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Iceberg::IcebergObjectSerializableInfo deserialized;
    deserialized.deserializeForClusterFunctionProtocol(read_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    ASSERT_TRUE(read_buffer.eof());
    ASSERT_EQ(deserialized.data_object_file_path_key, info.data_object_file_path_key);
    ASSERT_EQ(deserialized.underlying_format_read_schema_id, info.underlying_format_read_schema_id);
    ASSERT_EQ(deserialized.schema_id_relevant_to_iterator, info.schema_id_relevant_to_iterator);
    ASSERT_EQ(deserialized.sequence_number, info.sequence_number);
    ASSERT_EQ(deserialized.file_format, info.file_format);
    ASSERT_EQ(deserialized.position_deletes_objects.size(), 1);
    ASSERT_EQ(deserialized.equality_deletes_objects.size(), 1);
    ASSERT_TRUE(deserialized.record_count.has_value());
    ASSERT_EQ(*deserialized.record_count, 100500);
    ASSERT_TRUE(deserialized.file_size_in_bytes.has_value());
    ASSERT_EQ(*deserialized.file_size_in_bytes, 999888777);
}


TEST(DatalakeStateSerde, IcebergObjectSerializableInfoWithoutFileStats)
{
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = DB::Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/path/to/file.parquet");
    info.underlying_format_read_schema_id = 1;
    info.schema_id_relevant_to_iterator = 1;
    info.sequence_number = 0;
    info.file_format = "PARQUET";
    info.record_count = 42;
    info.file_size_in_bytes = 1024;

    /// Serialize at protocol version 5 (before file stats were added)
    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.serializeForClusterFunctionProtocol(write_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS);
    }

    /// Deserialize at protocol version 5 — the new fields should be absent
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Iceberg::IcebergObjectSerializableInfo deserialized;
    deserialized.deserializeForClusterFunctionProtocol(read_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS);

    ASSERT_TRUE(read_buffer.eof());
    ASSERT_EQ(deserialized.data_object_file_path_key, info.data_object_file_path_key);
    ASSERT_FALSE(deserialized.record_count.has_value());
    ASSERT_FALSE(deserialized.file_size_in_bytes.has_value());
}


TEST(DatalakeStateSerde, IcebergObjectSerializableInfoNulloptFileStats)
{
    /// Test round-trip when the fields are explicitly nullopt
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = DB::Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/path/to/file.parquet");
    info.underlying_format_read_schema_id = 1;
    info.schema_id_relevant_to_iterator = 1;
    info.sequence_number = 0;
    info.file_format = "PARQUET";
    info.record_count = std::nullopt;
    info.file_size_in_bytes = std::nullopt;

    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.serializeForClusterFunctionProtocol(write_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Iceberg::IcebergObjectSerializableInfo deserialized;
    deserialized.deserializeForClusterFunctionProtocol(read_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    ASSERT_TRUE(read_buffer.eof());
    ASSERT_FALSE(deserialized.record_count.has_value());
    ASSERT_FALSE(deserialized.file_size_in_bytes.has_value());
}

#if USE_LANCE
namespace
{
Lance::TableStateSnapshot makeLanceSnapshot(UInt64 version, UInt8 seed = 1)
{
    Lance::TableStateSnapshot snapshot;
    snapshot.version = version;
    snapshot.manifest_id.fill(seed);
    snapshot.manifest_size = 4096;
    snapshot.manifest_sha256.fill(seed + 1);
    return snapshot;
}
}

TEST(DatalakeStateSerde, LanceObjectSerializableInfoRoundTrip)
{
    Lance::LanceObjectSerializableInfo info;
    info.snapshot = makeLanceSnapshot(7);
    info.fragment_ids = {1, 3, 5, 8};
    info.pack_index = 2;
    info.pack_count = 4;

    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.serializeForClusterFunctionProtocol(write_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Lance::LanceObjectSerializableInfo deserialized;
    deserialized.deserializeForClusterFunctionProtocol(read_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    ASSERT_TRUE(read_buffer.eof());
    ASSERT_EQ(deserialized.snapshot, info.snapshot);
    ASSERT_EQ(deserialized.fragment_ids, info.fragment_ids);
    ASSERT_EQ(deserialized.pack_index, info.pack_index);
    ASSERT_EQ(deserialized.pack_count, info.pack_count);
}

TEST(DatalakeStateSerde, LanceObjectSerializableInfoEmptyFragments)
{
    Lance::LanceObjectSerializableInfo info;
    info.snapshot = makeLanceSnapshot(1);
    info.fragment_ids = {};
    info.pack_index = 0;
    info.pack_count = 1;

    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.serializeForClusterFunctionProtocol(write_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Lance::LanceObjectSerializableInfo deserialized;
    deserialized.deserializeForClusterFunctionProtocol(
        read_buffer, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    ASSERT_TRUE(read_buffer.eof());
    ASSERT_EQ(deserialized.snapshot, info.snapshot);
    ASSERT_TRUE(deserialized.fragment_ids.empty());
    ASSERT_EQ(deserialized.pack_count, 1u);
}

TEST(DatalakeStateSerde, LanceObjectSerializableInfoRejectsVersionOnlyProtocol)
{
    Lance::LanceObjectSerializableInfo info;
    info.snapshot = makeLanceSnapshot(1);

    String str;
    WriteBufferFromString write_buffer{str};
    EXPECT_THROW(
        info.serializeForClusterFunctionProtocol(
            write_buffer,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_METADATA),
        Exception);
}

TEST(DatalakeStateSerde, LanceObjectSerializableInfoRejectsInvalidPack)
{
    Lance::LanceObjectSerializableInfo info;
    info.snapshot = makeLanceSnapshot(1);
    info.pack_index = 1;
    info.pack_count = 1;

    String str;
    {
        WriteBufferFromString write_buffer{str};
        info.snapshot.serialize(write_buffer);
        writeVarUInt(0, write_buffer);
        writeVarUInt(1, write_buffer);
        writeVarUInt(1, write_buffer);
    }
    ReadBufferFromMemory read_buffer(str.data(), str.size());
    Lance::LanceObjectSerializableInfo deserialized;
    EXPECT_THROW(
        deserialized.deserializeForClusterFunctionProtocol(
            read_buffer,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION),
        Exception);
}

TEST(DatalakeStateSerde, LanceDatasetObjectInfoFromSerializableInfo)
{
    Lance::LanceObjectSerializableInfo info;
    info.snapshot = makeLanceSnapshot(9);
    info.fragment_ids = {10, 20};
    info.pack_index = 1;
    info.pack_count = 3;

    auto object = std::make_shared<LanceDatasetObjectInfo>(RelativePathWithMetadata{"s3://b/ds.lance#v9/pack1_f10"}, info);
    ASSERT_EQ(object->snapshot.version, 9u);
    ASSERT_EQ(object->fragment_ids, info.fragment_ids);
    ASSERT_FALSE(static_cast<bool>(object->dataset));
    ASSERT_EQ(object->pack_index, 1u);
    ASSERT_EQ(object->pack_count, 3u);

    auto round_trip = object->toSerializableInfo();
    ASSERT_EQ(round_trip.snapshot, info.snapshot);
    ASSERT_EQ(round_trip.fragment_ids, info.fragment_ids);
}
#endif
