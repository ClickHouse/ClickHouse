#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `quota_limits` system table, which allows you to get information about the limits set for quotas.
class StorageSystemQuotaLimits final : public ext::shared_ptr_helper<StorageSystemQuotaLimits>, public IStorageSystemOneBlock<StorageSystemQuotaLimits>
{
public:
    std::string getName() const override { return "SystemQuotaLimits"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemQuotaLimits>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
