#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas_usage` system table, which allows you to get information about
  * how all users use the quotas.
  */
class StorageSystemQuotasUsage final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemQuotasUsage"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
