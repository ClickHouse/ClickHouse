#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Information about macros for introspection.
  */
class StorageSystemMacros final : public IStorageSystemOneBlock<StorageSystemMacros>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemMacros> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemMacros>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemMacros(CreatePasskey, TArgs &&... args) : StorageSystemMacros{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemMacros"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
