#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
namespace DB
{

class StorageSystemTableFunctions final : public IStorageSystemOneBlock<StorageSystemTableFunctions>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemTableFunctions> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemTableFunctions>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemTableFunctions(CreatePasskey, TArgs &&... args) : StorageSystemTableFunctions{std::forward<TArgs>(args)...}
    {
    }

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
