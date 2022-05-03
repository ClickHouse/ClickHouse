#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas` system tables, which allows you to get information about quotas.
  */
class StorageSystemQuotas final : public IStorageSystemOneBlock<StorageSystemQuotas>
{
public:
    std::string getName() const override { return "SystemQuotas"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
