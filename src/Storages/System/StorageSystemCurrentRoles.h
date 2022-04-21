#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `current_roles` system table, which allows you to get information about current roles.
class StorageSystemCurrentRoles final : public IStorageSystemOneBlock<StorageSystemCurrentRoles>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemCurrentRoles> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemCurrentRoles>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemCurrentRoles(CreatePasskey, TArgs &&... args) : StorageSystemCurrentRoles{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemCurrentRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
