#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;
struct QuotaUsage;


/** Implements the `quota_usage` system table, which allows you to get information about
  * how the current user uses the quota.
  */
class StorageSystemQuotaUsage final : public IStorageSystemOneBlock<StorageSystemQuotaUsage>
{
public:
    std::string getName() const override { return "SystemQuotaUsage"; }
    static NamesAndTypesList getNamesAndTypes();

    static NamesAndTypesList getNamesAndTypesImpl(bool add_column_is_current);
    static void fillDataImpl(MutableColumns & res_columns, ContextPtr context, bool add_column_is_current, const std::vector<QuotaUsage> & quotas_usage);

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
