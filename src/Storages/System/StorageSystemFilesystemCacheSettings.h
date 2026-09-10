#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{

class StorageSystemFilesystemCacheSettings final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemFilesystemCacheSettings(const StorageID & table_id_);

    std::string getName() const override { return "SystemFilesystemCacheSettings"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
