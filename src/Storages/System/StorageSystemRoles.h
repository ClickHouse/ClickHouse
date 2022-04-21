#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `roles` system table, which allows you to get information about roles.
class StorageSystemRoles final : public IStorageSystemOneBlock<StorageSystemRoles>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemRoles> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemRoles>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemRoles(CreatePasskey, TArgs &&... args) : StorageSystemRoles{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
