#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemPartAggregationCache final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemPartAggregationCache(const StorageID & table_id);

    std::string getName() const override { return "SystemPartAggregationCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
