#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.background_schedule_pool table - shows all tasks from all BackgroundSchedulePool instances
class StorageSystemBackgroundSchedulePool final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemBackgroundSchedulePool"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
