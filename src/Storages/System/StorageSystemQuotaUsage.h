#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;
struct QuotaUsage;


/** Implements the `quota_usage` system table, which allows you to get information about
  * how the current user uses the quota.
  */
class StorageSystemQuotaUsage final : public ext::shared_ptr_helper<StorageSystemQuotaUsage>, public IStorageSystemOneBlock<StorageSystemQuotaUsage>
{
public:
    std::string getName() const override { return "SystemQuotaUsage"; }
    static NamesAndTypesList getNamesAndTypes();

    static NamesAndTypesList getNamesAndTypesImpl(bool add_column_is_current);
    static void fillDataImpl(MutableColumns & res_columns, const Context & context, bool add_column_is_current, const std::vector<QuotaUsage> & quotas_usage);

protected:
    friend struct ext::shared_ptr_helper<StorageSystemQuotaUsage>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
