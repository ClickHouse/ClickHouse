#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases final : public IStorageSystemOneBlock<StorageSystemDatabases>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDatabases> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDatabases>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDatabases(CreatePasskey, TArgs &&... args) : StorageSystemDatabases{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override
    {
        return "SystemDatabases";
    }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
