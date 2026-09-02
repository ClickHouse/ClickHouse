#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `system.scheduler` table, which allows you to get information about scheduling nodes.
class StorageSystemScheduler final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemScheduler"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
