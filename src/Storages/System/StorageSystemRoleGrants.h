#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `role_grants` system table, which allows you to get information about granted roles.
class StorageSystemRoleGrants final : public ext::shared_ptr_helper<StorageSystemRoleGrants>, public IStorageSystemOneBlock<StorageSystemRoleGrants>
{
public:
    std::string getName() const override { return "SystemRoleGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemRoleGrants>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
