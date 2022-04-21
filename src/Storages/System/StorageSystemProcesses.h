#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `processes` system table, which allows you to get information about the queries that are currently executing.
  */
class StorageSystemProcesses final : public IStorageSystemOneBlock<StorageSystemProcesses>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemProcesses> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemProcesses>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemProcesses(CreatePasskey, TArgs &&... args) : StorageSystemProcesses{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemProcesses"; }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
