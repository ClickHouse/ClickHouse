#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;


/** System table "contributors" with list of clickhouse contributors
  */
class StorageSystemContributors final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemContributors";
    }

    static ColumnsDescription getColumnsDescription();
};
}
