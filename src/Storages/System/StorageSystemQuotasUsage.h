#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas_usage` system table, which allows you to get information about
  * how all users use the quotas.
  */
class StorageSystemQuotasUsage final : public shared_ptr_helper<StorageSystemQuotasUsage>, public IStorageSystemOneBlock<StorageSystemQuotasUsage>
{
public:
    std::string getName() const override { return "SystemQuotasUsage"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemQuotasUsage>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
