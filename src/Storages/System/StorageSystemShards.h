#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// `system.shards` — SQL `CREATE SHARD` definitions from the local catalog (`ClusterFactory`).
class StorageSystemShards final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemShards"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const override;
};

}
