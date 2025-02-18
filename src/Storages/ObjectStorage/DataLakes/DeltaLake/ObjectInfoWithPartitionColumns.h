#pragma once
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

/**
 * A class which adds partition values info to the DB::ObjectInfo.
 * As DeltaLake does not store parition columns values in the actual data files,
 * but instead in the data files directory names,
 * we need a way to pass the value through to the StorageObjectStorageSource.
 */
struct ObjectInfoWithParitionColumns : public DB::ObjectInfo
{
    struct PartitionColumnInfo
    {
        /// Name and type of the partition column.
        NameAndTypePair name_and_type;
        /// Partition value.
        Field value;
    };
    using PartitionColumnsInfo = std::vector<PartitionColumnInfo>;

    template <typename... Args>
    explicit ObjectInfoWithParitionColumns(
        PartitionColumnsInfo && partitions_info_, Args &&... args)
        : DB::ObjectInfo(std::forward<Args>(args)...)
        , partitions_info(partitions_info_) {}

    PartitionColumnsInfo partitions_info;
};

}
