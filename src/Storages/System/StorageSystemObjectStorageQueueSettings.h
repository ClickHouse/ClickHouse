#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Disks/DiskType.h>

namespace DB
{

class Context;

template <ObjectStorageType type>
class StorageSystemObjectStorageQueueSettings final : public IStorageSystemOneBlock
{
public:
    static constexpr auto name = type == ObjectStorageType::S3 ? "SystemS3QueueSettings" : "SystemAzureQueueSettings";

    std::string getName() const override { return name; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(
        MutableColumns & res_columns,
        ContextPtr context,
        const ActionsDAG::Node *,
        std::vector<UInt8>) const override;
};

}
