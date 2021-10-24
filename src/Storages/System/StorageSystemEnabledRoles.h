#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `enabled_roles` system table, which allows you to get information about enabled roles.
class StorageSystemEnabledRoles final : public shared_ptr_helper<StorageSystemEnabledRoles>, public IStorageSystemOneBlock<StorageSystemEnabledRoles>
{
public:
    std::string getName() const override { return "SystemEnabledRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemEnabledRoles>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
