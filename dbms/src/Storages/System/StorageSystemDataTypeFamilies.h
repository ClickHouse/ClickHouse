#pragma once

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{

class StorageSystemDataTypeFamilies : public ext::shared_ptr_helper<StorageSystemDataTypeFamilies>,
                                      public IStorageSystemOneBlock<StorageSystemDataTypeFamilies>
{
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemTableDataTypeFamilies"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
