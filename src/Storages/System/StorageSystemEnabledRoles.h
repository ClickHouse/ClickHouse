#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `enabled_roles` system table, which allows you to get information about enabled roles.
class StorageSystemEnabledRoles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemEnabledRoles"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
