#pragma once
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

/**
 * A class which adds partition values info to the DB::ObjectInfo.
 * As DeltaLake does not store partition columns values in the actual data files,
 * but instead in the data files directory names,
 * we need a way to pass the value through to the StorageObjectStorageSource.
 */
struct ObjectInfoWithPartitionColumns : public DB::ObjectInfo
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
    explicit ObjectInfoWithPartitionColumns(
        PartitionColumnsInfo && partitions_info_, Args &&... args)
        : DB::ObjectInfo(std::forward<Args>(args)...)
        , partitions_info(partitions_info_) {}

    ~ObjectInfoWithPartitionColumns() override = default;

    PartitionColumnsInfo partitions_info;
};

}
