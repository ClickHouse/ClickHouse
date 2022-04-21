#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users` system table, which allows you to get information about users.
class StorageSystemUsers final : public IStorageSystemOneBlock<StorageSystemUsers>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemUsers> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemUsers>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemUsers(CreatePasskey, TArgs &&... args) : StorageSystemUsers{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemUsers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
