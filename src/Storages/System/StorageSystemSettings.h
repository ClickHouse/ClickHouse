#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemSettings final : public shared_ptr_helper<StorageSystemSettings>, public IStorageSystemOneBlock<StorageSystemSettings>
{
    friend struct shared_ptr_helper<StorageSystemSettings>;
public:
    std::string getName() const override { return "SystemSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
