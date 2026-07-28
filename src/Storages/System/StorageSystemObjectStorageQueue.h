#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Disks/DiskType.h>

namespace DB
{

template <ObjectStorageType type>
class StorageSystemObjectStorageQueue final : public IStorageSystemOneBlock
{
public:
    static constexpr auto name = type == ObjectStorageType::S3 ? "SystemS3Queue" : "SystemAzureQueue";

    explicit StorageSystemObjectStorageQueue(const StorageID & table_id_);

    std::string getName() const override { return name; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
