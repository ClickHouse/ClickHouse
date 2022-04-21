#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `privileges` system table, which allows you to get information about access types.
class StorageSystemPrivileges final : public IStorageSystemOneBlock<StorageSystemPrivileges>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemPrivileges> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemPrivileges>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemPrivileges(CreatePasskey, TArgs &&... args) : StorageSystemPrivileges{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemPrivileges"; }
    static NamesAndTypesList getNamesAndTypes();
    static const std::vector<std::pair<String, Int16>> & getAccessTypeEnumValues();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
