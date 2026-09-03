#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemBackups final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemBackups"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
