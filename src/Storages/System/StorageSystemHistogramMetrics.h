#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `histogram_metrics` system table.
  */
class StorageSystemHistogramMetrics final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemHistogramMetrics"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
