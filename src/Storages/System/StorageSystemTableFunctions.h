#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>
namespace DB
{

class StorageSystemTableFunctions final : public shared_ptr_helper<StorageSystemTableFunctions>,
                                    public IStorageSystemOneBlock<StorageSystemTableFunctions>
{
    friend struct shared_ptr_helper<StorageSystemTableFunctions>;
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
