#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `role_grants` system table, which allows you to get information about granted roles.
class StorageSystemRoleGrants final : public IStorageSystemOneBlock<StorageSystemRoleGrants>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemRoleGrants> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemRoleGrants>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemRoleGrants(CreatePasskey, TArgs &&... args) : StorageSystemRoleGrants{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemRoleGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
