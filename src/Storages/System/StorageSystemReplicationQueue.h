#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `replication_queue` system table, which allows you to view the replication queues for the replicated tables.
  */
class StorageSystemReplicationQueue final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemReplicationQueue"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
