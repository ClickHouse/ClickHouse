#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemSettings final : public StorageHelper<StorageSystemSettings>, public IStorageSystemOneBlock<StorageSystemSettings>
{
    friend struct StorageHelper<StorageSystemSettings>;
public:
    std::string getName() const override { return "SystemSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
