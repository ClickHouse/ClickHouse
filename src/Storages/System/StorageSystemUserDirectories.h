#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users_directories` system table, which allows you to get information about user directories.
class StorageSystemUserDirectories final : public IStorageSystemOneBlock<StorageSystemUserDirectories>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemUserDirectories> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemUserDirectories>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemUserDirectories(CreatePasskey, TArgs &&... args) : StorageSystemUserDirectories{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemUserDirectories"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
