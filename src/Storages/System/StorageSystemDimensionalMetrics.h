#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `dimensional_metrics` system table.
  */
class StorageSystemDimensionalMetrics final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDimensionalMetrics"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
