#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `quota_limits` system table, which allows you to get information about the limits set for quotas.
class StorageSystemQuotaLimits final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemQuotaLimits"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
