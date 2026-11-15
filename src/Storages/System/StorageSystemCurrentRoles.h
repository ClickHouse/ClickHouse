#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `current_roles` system table, which allows you to get information about current roles.
class StorageSystemCurrentRoles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemCurrentRoles"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
