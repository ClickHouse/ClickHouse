#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Disks/DiskType.h>

namespace DB
{

/// Shows, for each registered ObjectStorageQueue metadata object, the current
/// number of processed/processing/failed nodes stored in Keeper.
template <ObjectStorageType type>
class StorageSystemObjectStorageQueueMetadata final : public IStorageSystemOneBlock
{
public:
    static constexpr auto name = type == ObjectStorageType::S3 ? "SystemS3QueueMetadata" : "SystemAzureQueueMetadata";

    explicit StorageSystemObjectStorageQueueMetadata(const StorageID & table_id_);

    std::string getName() const override { return name; }

    static ColumnsDescription getColumnsDescription();

protected:
    bool supportsColumnsMask() const override { return true; }

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
