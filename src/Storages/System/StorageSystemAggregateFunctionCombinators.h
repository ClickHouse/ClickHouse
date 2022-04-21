#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemAggregateFunctionCombinators final : public IStorageSystemOneBlock<StorageSystemAggregateFunctionCombinators>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemAggregateFunctionCombinators> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemAggregateFunctionCombinators>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemAggregateFunctionCombinators(CreatePasskey, TArgs &&... args) : StorageSystemAggregateFunctionCombinators{std::forward<TArgs>(args)...}
    {
    }

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
