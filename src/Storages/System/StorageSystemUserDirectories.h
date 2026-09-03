#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users_directories` system table, which allows you to get information about user directories.
class StorageSystemUserDirectories final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemUserDirectories"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
