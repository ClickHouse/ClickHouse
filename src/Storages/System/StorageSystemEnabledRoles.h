#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `enabled_roles` system table, which allows you to get information about enabled roles.
class StorageSystemEnabledRoles final : public IStorageSystemOneBlock<StorageSystemEnabledRoles>
{
public:
    std::string getName() const override { return "SystemEnabledRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
