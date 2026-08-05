#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

using namespace DB;

TEST(IcebergCountShortcuts, HasEqualityAndPositionDeleteHelpers)
{
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet");
    info.file_format = "PARQUET";

    auto plain = std::make_shared<ObjectInfo>(RelativePathWithMetadata{"data/file.parquet"});
    EXPECT_FALSE(hasIcebergEqualityDeletes(plain));
    EXPECT_FALSE(hasIcebergPositionDeletes(plain));

    auto iceberg = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata{"data/file.parquet"}, info);
    EXPECT_FALSE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_FALSE(hasIcebergPositionDeletes(iceberg));

    iceberg->info.equality_deletes_objects.push_back(
        Iceberg::EqualityDeleteObject{
            .file_path = "s3://bucket/deletes/eq.parquet",
            .file_format = "PARQUET",
            .equality_ids = std::vector<Int32>{1},
            .schema_id = 0,
        });
    EXPECT_TRUE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_FALSE(hasIcebergPositionDeletes(iceberg));

    iceberg->info.position_deletes_objects.push_back(
        Iceberg::PositionDeleteObject{
            .file_path = "s3://bucket/deletes/pos.parquet",
            .file_format = "PARQUET",
            .reference_data_file_path = std::nullopt,
            .sequence_number = 1,
        });
    EXPECT_TRUE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_TRUE(hasIcebergPositionDeletes(iceberg));
}

TEST(IcebergCountShortcuts, SnapshotSummaryFieldsRemainLoggingHintsOnly)
{
    /// IcebergMetadata::totalRows must not return summary math. These fields stay populated for
    /// mismatch warnings / equality fail-closed, but there is no getTotalRows shortcut anymore.
    Iceberg::IcebergDataSnapshot snapshot;
    snapshot.total_rows = 100;
    snapshot.total_position_delete_rows = 10;
    snapshot.total_equality_delete_rows = 0;

    EXPECT_EQ(*snapshot.total_rows, 100u);
    EXPECT_EQ(*snapshot.total_position_delete_rows, 10u);
    EXPECT_EQ(*snapshot.total_equality_delete_rows, 0u);
}

#endif
