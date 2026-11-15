#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `role_grants` system table, which allows you to get information about granted roles.
class StorageSystemRoleGrants final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemRoleGrants"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
