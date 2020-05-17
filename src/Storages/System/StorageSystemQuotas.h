#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas` system tables, which allows you to get information about quotas.
  */
class StorageSystemQuotas final : public ext::shared_ptr_helper<StorageSystemQuotas>, public IStorageSystemOneBlock<StorageSystemQuotas>
{
public:
    std::string getName() const override { return "SystemQuotas"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemQuotas>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
