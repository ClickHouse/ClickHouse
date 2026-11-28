#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `distributed_partition_moves` system table, which shows the partition moves loaded by the Distributed tables.
  */
class StorageSystemDistributedPartitionMoves final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDistributedPartitionMoves"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
