#pragma once
#include "config.h"

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{

class StorageSystemS3Queue final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemS3Queue(const StorageID & table_id_);

    std::string getName() const override { return "SystemS3Queue"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
