#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemQueryResultCache final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemQueryResultCache(const StorageID & table_id);

    std::string getName() const override { return "SystemQueryResultCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
