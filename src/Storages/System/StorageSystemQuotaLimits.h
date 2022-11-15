#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `quota_limits` system table, which allows you to get information about the limits set for quotas.
class StorageSystemQuotaLimits final : public IStorageSystemOneBlock<StorageSystemQuotaLimits>
{
public:
    std::string getName() const override { return "SystemQuotaLimits"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
