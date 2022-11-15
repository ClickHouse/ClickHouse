#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `role_grants` system table, which allows you to get information about granted roles.
class StorageSystemRoleGrants final : public IStorageSystemOneBlock<StorageSystemRoleGrants>
{
public:
    std::string getName() const override { return "SystemRoleGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
