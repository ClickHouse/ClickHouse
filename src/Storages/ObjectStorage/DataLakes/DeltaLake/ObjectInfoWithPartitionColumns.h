#pragma once
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

struct ObjectInfoWithParitionColumns : public DB::ObjectInfo
{
    struct PartitionColumnInfo
    {
        NameAndTypePair name_and_type;
        Field value;
    };
    using PartitionColumnsInfo = std::vector<PartitionColumnInfo>;

    template <typename... Args>
    explicit ObjectInfoWithParitionColumns(
        PartitionColumnsInfo && partitions_info_, Args &&... args)
        : DB::ObjectInfo(std::forward<Args>(args)...)
        , partitions_info(partitions_info_)
    {
    }

    PartitionColumnsInfo partitions_info;
};

}
