#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** System table "jemalloc_stats" that returns jemalloc statistics.
  * This table returns a single row with a single column containing the output of malloc_stats_print.
  */
class StorageSystemJemallocStats final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:

    std::string getName() const override { return "SystemJemallocStats"; }

    static ColumnsDescription getColumnsDescription();
};

}
