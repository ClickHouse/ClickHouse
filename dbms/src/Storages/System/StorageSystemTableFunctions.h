#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{

class StorageSystemTableFunctions final : public StorageHelper<StorageSystemTableFunctions>,
                                    public IStorageSystemOneBlock<StorageSystemTableFunctions>
{
    friend struct StorageHelper<StorageSystemTableFunctions>;
protected:

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

public:

    std::string getName() const override
    {
        return "SystemTableFunctions";
    }

    static NamesAndTypesList getNamesAndTypes();

};

}
