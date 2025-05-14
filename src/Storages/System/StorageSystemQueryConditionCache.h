#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemQueryConditionCache final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemQueryConditionCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemQueryConditionCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
