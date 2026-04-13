#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemReplicasCollection final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemReplicasCollection(const StorageID & table_id_);

    std::string getName() const override { return "SystemReplicasCollection"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
