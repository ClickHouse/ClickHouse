#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
class StorageSystemAggregateFunctionCombinators final : public shared_ptr_helper<StorageSystemAggregateFunctionCombinators>,
                                                  public IStorageSystemOneBlock<StorageSystemAggregateFunctionCombinators>
{
    friend struct shared_ptr_helper<StorageSystemAggregateFunctionCombinators>;
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override
    {
        return "SystemAggregateFunctionCombinators";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
