#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemAggregateFunctionCombinators : public ext::shared_ptr_helper<StorageSystemAggregateFunctionCombinators>,
                                                  public IStorageSystemOneBlock<StorageSystemAggregateFunctionCombinators>
{
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override
    {
        return "SystemAggregateFunctionCombinators";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
