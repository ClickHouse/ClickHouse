#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{

class StorageSystemTableFunctions final : public ext::shared_ptr_helper<StorageSystemTableFunctions>,
                                    public IStorageSystemOneBlock<StorageSystemTableFunctions>
{
    friend struct ext::shared_ptr_helper<StorageSystemTableFunctions>;
protected:

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

public:

    std::string getName() const override
    {
        return "SystemTableFunctions";
    }

    static NamesAndTypesList getNamesAndTypes();

};

}
