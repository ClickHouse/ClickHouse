#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemGrants final : public IStorageSystemOneBlock<StorageSystemGrants>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemGrants> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemGrants>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemGrants(CreatePasskey, TArgs &&... args) : StorageSystemGrants{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
