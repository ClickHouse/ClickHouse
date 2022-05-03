#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas_usage` system table, which allows you to get information about
  * how all users use the quotas.
  */
class StorageSystemQuotasUsage final : public IStorageSystemOneBlock<StorageSystemQuotasUsage>
{
public:
    std::string getName() const override { return "SystemQuotasUsage"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
