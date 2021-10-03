#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "licenses" with list of licenses of 3rd party libraries
  */
class StorageSystemLicenses final :
    public shared_ptr_helper<StorageSystemLicenses>,
    public IStorageSystemOneBlock<StorageSystemLicenses>
{
    friend struct shared_ptr_helper<StorageSystemLicenses>;
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemLicenses";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
