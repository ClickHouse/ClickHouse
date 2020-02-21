#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `quota_usage` system tables, which allows you to get information about
  * how the quotas are used by all users.
  */
class StorageSystemQuotaUsage : public ext::shared_ptr_helper<StorageSystemQuotaUsage>, public IStorageSystemOneBlock<StorageSystemQuotaUsage>
{
public:
    std::string getName() const override { return "SystemQuotaUsage"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemQuotaUsage>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
