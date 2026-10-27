#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Disks/DiskType.h>

namespace DB
{

/// Shows, for each registered ObjectStorageQueue metadata object, the state
/// stored in Keeper: the number of processed/processing/failed nodes, their
/// contents on demand, and (in ordered mode) the last-processed pointers.
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

    /// `zookeeper_path` is filterable so a targeted query does not touch (and
    /// potentially fail on) unrelated, possibly unhealthy, queues in Keeper.
    Block getFilterSampleBlock() const override;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
