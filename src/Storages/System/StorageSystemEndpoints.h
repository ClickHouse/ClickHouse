#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// `system.endpoints` — SQL `CREATE ENDPOINT` definitions from `ClusterMetadataManager`.
class StorageSystemEndpoints final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemEndpoints"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const override;
};

}
