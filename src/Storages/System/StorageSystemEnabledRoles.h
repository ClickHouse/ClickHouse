#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `enabled_roles` system table, which allows you to get information about enabled roles.
class StorageSystemEnabledRoles final : public IStorageSystemOneBlock<StorageSystemEnabledRoles>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemEnabledRoles> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemEnabledRoles>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemEnabledRoles(CreatePasskey, TArgs &&... args) : StorageSystemEnabledRoles{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemEnabledRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
