#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/Exception.h>
#include <Core/NamesAndTypes.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <config.h>

#if USE_PARQUET
#include <Common/tests/gtest_global_register.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#endif

using namespace DB;

namespace DB::ErrorCodes
{
extern const int UNKNOWN_PROTOCOL;
}

/// Cluster-protocol fail-closed behavior for Iceberg deletion vectors / equality / position
/// deletes and file buckets. Master-only APIs (`read_source_index`,
/// `derive_file_name_from_url_path`, `getIdentifier(bool)`) are intentionally not covered here.

TEST(ClusterFunctionReadTaskResponse, RejectsSchemaTransformOnProtocolBeforeDataLakeMetadata)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.schema_transform
        = std::make_shared<ActionsDAG>(NamesAndTypesList{{"x", std::make_shared<DataTypeUInt64>()}});

    String serialized;
    WriteBufferFromString out(serialized);
    try
    {
        response.serialize(out, DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PROTOCOL);
        EXPECT_NE(e.message().find("schema_transform"), std::string::npos);
    }
}

TEST(ClusterFunctionReadTaskResponse, AllowsEmptySchemaTransformOnProtocolBeforeDataLakeMetadata)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.schema_transform = std::make_shared<ActionsDAG>();

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION);
    out.finalize();
    EXPECT_FALSE(serialized.empty());
}

TEST(ClusterFunctionReadTaskResponse, RoundTripsSchemaTransformOnSupportedProtocol)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.schema_transform
        = std::make_shared<ActionsDAG>(NamesAndTypesList{{"x", std::make_shared<DataTypeUInt64>()}});

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA);
    out.finalize();

    ReadBufferFromString in(serialized);
    ClusterFunctionReadTaskResponse deserialized;
    deserialized.deserialize(in);

    ASSERT_TRUE(deserialized.data_lake_metadata.schema_transform);
    EXPECT_FALSE(deserialized.data_lake_metadata.schema_transform->getInputs().empty());
}

TEST(ClusterFunctionReadTaskResponse, RejectsNonEmptyExcludedRowsOnOldProtocol)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    response.data_lake_metadata.excluded_rows->add(7);

    String serialized;
    WriteBufferFromString out(serialized);
    try
    {
        response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PROTOCOL);
        EXPECT_NE(e.message().find("excluded_rows"), std::string::npos);
    }
}

TEST(ClusterFunctionReadTaskResponse, AllowsEmptyExcludedRowsOnOldProtocol)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO);
    out.finalize();
    EXPECT_FALSE(serialized.empty());
}

TEST(ClusterFunctionReadTaskResponse, RoundTripsExcludedRowsOnSupportedProtocol)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.data_lake_metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    response.data_lake_metadata.excluded_rows->add(3);
    response.data_lake_metadata.excluded_rows->add(9);

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS);
    out.finalize();

    ReadBufferFromString in(serialized);
    ClusterFunctionReadTaskResponse deserialized;
    deserialized.deserialize(in);

    ASSERT_TRUE(deserialized.data_lake_metadata.excluded_rows);
    EXPECT_EQ(deserialized.data_lake_metadata.excluded_rows->size(), 2u);
    EXPECT_TRUE(deserialized.data_lake_metadata.excluded_rows->rb_contains(3));
    EXPECT_TRUE(deserialized.data_lake_metadata.excluded_rows->rb_contains(9));
}

TEST(ClusterFunctionReadTaskResponse, RejectsEqualityDeletesOnProtocolBeforeIcebergMetadata)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
    response.iceberg_info->equality_deletes_objects.push_back(
        Iceberg::EqualityDeleteObject{
            .file_path = "/path/eq.parquet",
            .file_format = "PARQUET",
            .equality_ids = std::vector<Int32>{1},
            .schema_id = 0,
        });

    String serialized;
    WriteBufferFromString out(serialized);
    try
    {
        response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PROTOCOL);
        EXPECT_NE(e.message().find("iceberg_info"), std::string::npos);
    }
}

TEST(ClusterFunctionReadTaskResponse, RejectsPositionDeletesOnProtocolBeforeIcebergMetadata)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
    response.iceberg_info->position_deletes_objects.push_back(
        Iceberg::PositionDeleteObject{
            .file_path = "/path/pos.parquet",
            .file_format = "PARQUET",
            .reference_data_file_path = std::nullopt,
            .sequence_number = 1,
        });

    String serialized;
    WriteBufferFromString out(serialized);
    try
    {
        response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PROTOCOL);
        EXPECT_NE(e.message().find("iceberg_info"), std::string::npos);
    }
}

TEST(ClusterFunctionReadTaskResponse, AllowsIcebergInfoWithoutDeletesOnProtocolBeforeIcebergMetadata)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.iceberg_info = Iceberg::IcebergObjectSerializableInfo{};

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA);
    out.finalize();
    EXPECT_FALSE(serialized.empty());
}

TEST(ClusterFunctionReadTaskResponse, RoundTripsIcebergDeletesOnSupportedProtocol)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
    response.iceberg_info->data_object_file_path_key
        = Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/path/file.parquet");
    response.iceberg_info->file_format = "PARQUET";
    response.iceberg_info->equality_deletes_objects.push_back(
        Iceberg::EqualityDeleteObject{
            .file_path = "/path/eq.parquet",
            .file_format = "PARQUET",
            .equality_ids = std::vector<Int32>{1, 2},
            .schema_id = 7,
        });
    response.iceberg_info->position_deletes_objects.push_back(
        Iceberg::PositionDeleteObject{
            .file_path = "/path/pos.parquet",
            .file_format = "PARQUET",
            .reference_data_file_path = std::nullopt,
            .sequence_number = 42,
        });

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA);
    out.finalize();

    ReadBufferFromString in(serialized);
    ClusterFunctionReadTaskResponse deserialized;
    deserialized.deserialize(in);

    ASSERT_TRUE(deserialized.iceberg_info.has_value());
    ASSERT_EQ(deserialized.iceberg_info->equality_deletes_objects.size(), 1u);
    EXPECT_EQ(deserialized.iceberg_info->equality_deletes_objects[0].file_path, "/path/eq.parquet");
    ASSERT_EQ(deserialized.iceberg_info->position_deletes_objects.size(), 1u);
    EXPECT_EQ(deserialized.iceberg_info->position_deletes_objects[0].file_path, "/path/pos.parquet");
}

#if USE_PARQUET

TEST(ClusterFunctionReadTaskResponse, RejectsFileBucketInfoOnProtocolBeforeFileBuckets)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.file_bucket_info = std::make_shared<ParquetFileBucketInfo>(std::vector<size_t>{0, 1});

    String serialized;
    WriteBufferFromString out(serialized);
    try
    {
        response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PROTOCOL);
        EXPECT_NE(e.message().find("file_bucket_info"), std::string::npos);
    }
}

TEST(ClusterFunctionReadTaskResponse, AllowsMissingFileBucketInfoOnProtocolBeforeFileBuckets)
{
    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA);
    out.finalize();
    EXPECT_FALSE(serialized.empty());
}

TEST(ClusterFunctionReadTaskResponse, RoundTripsFileBucketInfoOnSupportedProtocol)
{
    tryRegisterFormats();

    ClusterFunctionReadTaskResponse response;
    response.path = "/path/file.parquet";
    response.file_bucket_info = std::make_shared<ParquetFileBucketInfo>(std::vector<size_t>{2, 5});

    String serialized;
    WriteBufferFromString out(serialized);
    response.serialize(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO);
    out.finalize();

    ReadBufferFromString in(serialized);
    ClusterFunctionReadTaskResponse deserialized;
    deserialized.deserialize(in);

    ASSERT_TRUE(deserialized.file_bucket_info);
    auto * parquet_buckets = dynamic_cast<ParquetFileBucketInfo *>(deserialized.file_bucket_info.get());
    ASSERT_TRUE(parquet_buckets);
    EXPECT_EQ(parquet_buckets->row_group_ids, (std::vector<size_t>{2, 5}));
}

#endif
