#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/PODArray.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

using namespace DB;

TEST(IcebergDataObjectInfoClone, PreservesDeletesAndDeletionVector)
{
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet");
    info.file_format = "PARQUET";
    info.equality_deletes_objects.push_back(
        Iceberg::EqualityDeleteObject{
            .file_path = "s3://bucket/deletes/eq.parquet",
            .file_format = "PARQUET",
            .equality_ids = std::vector<Int32>{1, 2},
            .schema_id = 7,
        });
    info.position_deletes_objects.push_back(
        Iceberg::PositionDeleteObject{
            .file_path = "s3://bucket/deletes/pos.parquet",
            .file_format = "PARQUET",
            .reference_data_file_path = std::nullopt,
            .sequence_number = 42,
        });
    info.deletion_vector = Iceberg::DeletionVectorObject{
        .file_path = "s3://bucket/deletes/dv.puffin",
        .content_offset = 100,
        .content_size_in_bytes = 50,
    };
    info.identity_partition_columns.emplace_back("id_bucket", Field{UInt64(3)});

    auto original = std::make_shared<IcebergDataObjectInfo>(
        RelativePathWithMetadata{"data/file.parquet"}, info);
    original->data_lake_metadata.emplace();
    original->data_lake_metadata->excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    original->data_lake_metadata->excluded_rows->add(11);
    auto rows_to_read = std::make_shared<PaddedPODArray<UInt64>>();
    rows_to_read->push_back(1);
    rows_to_read->push_back(2);
    rows_to_read->push_back(3);
    original->rows_to_read = std::move(rows_to_read);

    /// Object-slicing (the old `ObjectIteratorSplitByBuckets` path) drops Iceberg metadata.
    ObjectInfo sliced_value = *original;
    auto sliced = std::make_shared<ObjectInfo>(sliced_value);
    EXPECT_FALSE(std::dynamic_pointer_cast<IcebergDataObjectInfo>(sliced));
    ASSERT_TRUE(sliced->data_lake_metadata.has_value());
    ASSERT_TRUE(sliced->data_lake_metadata->excluded_rows);
    EXPECT_EQ(sliced->data_lake_metadata->excluded_rows->size(), 1u);

    auto cloned = original->clone();
    auto iceberg_cloned = std::dynamic_pointer_cast<IcebergDataObjectInfo>(cloned);
    ASSERT_TRUE(iceberg_cloned);
    ASSERT_EQ(iceberg_cloned->info.equality_deletes_objects.size(), 1u);
    EXPECT_EQ(iceberg_cloned->info.equality_deletes_objects[0].file_path, "s3://bucket/deletes/eq.parquet");
    ASSERT_TRUE(iceberg_cloned->info.equality_deletes_objects[0].equality_ids.has_value());
    EXPECT_EQ(iceberg_cloned->info.equality_deletes_objects[0].equality_ids->size(), 2u);
    ASSERT_EQ(iceberg_cloned->info.position_deletes_objects.size(), 1u);
    EXPECT_EQ(iceberg_cloned->info.position_deletes_objects[0].file_path, "s3://bucket/deletes/pos.parquet");
    EXPECT_EQ(iceberg_cloned->info.position_deletes_objects[0].sequence_number, 42);
    ASSERT_TRUE(iceberg_cloned->info.deletion_vector.has_value());
    EXPECT_EQ(iceberg_cloned->info.deletion_vector->file_path, "s3://bucket/deletes/dv.puffin");
    EXPECT_EQ(iceberg_cloned->info.deletion_vector->content_offset, 100);
    EXPECT_EQ(iceberg_cloned->info.deletion_vector->content_size_in_bytes, 50);
    ASSERT_EQ(iceberg_cloned->info.identity_partition_columns.size(), 1u);
    EXPECT_EQ(iceberg_cloned->info.identity_partition_columns[0].first, "id_bucket");
    ASSERT_TRUE(iceberg_cloned->data_lake_metadata.has_value());
    ASSERT_TRUE(iceberg_cloned->data_lake_metadata->excluded_rows);
    EXPECT_EQ(iceberg_cloned->data_lake_metadata->excluded_rows->size(), 1u);
    EXPECT_TRUE(iceberg_cloned->data_lake_metadata->excluded_rows->rb_contains(11));
    ASSERT_TRUE(iceberg_cloned->rows_to_read);
    ASSERT_EQ(iceberg_cloned->rows_to_read->size(), 3u);
    EXPECT_EQ((*iceberg_cloned->rows_to_read)[0], 1u);
}

#endif
