#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemTableFunctions : public ext::shared_ptr_helper<StorageSystemTableFunctions>,
                                    public IStorageSystemOneBlock<StorageSystemTableFunctions>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override
    {
        return "SystemTableFunctions";
    }

    static NamesAndTypesList getNamesAndTypes()
    {
        return {{"name", std::make_shared<DataTypeString>()}};
    }
};
}
