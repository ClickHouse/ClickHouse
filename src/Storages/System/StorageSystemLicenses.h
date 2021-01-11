#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "licenses" with list of licenses of 3rd party libraries
  */
class StorageSystemLicenses final :
    public ext::shared_ptr_helper<StorageSystemLicenses>,
    public IStorageSystemOneBlock<StorageSystemLicenses>
{
    friend struct ext::shared_ptr_helper<StorageSystemLicenses>;
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemLicenses";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
