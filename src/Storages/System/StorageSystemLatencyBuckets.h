#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
class Context;

class StorageSystemLatencyBuckets final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemLatencyBuckets";
    }

    static ColumnsDescription getColumnsDescription();
};
}
