#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the `replication_queue` system table, which allows you to view the replication queues for the replicated tables.
  */
class StorageSystemReplicationQueue : public ext::shared_ptr_helper<StorageSystemReplicationQueue>, public IStorage
{
public:
    std::string getName() const override { return "SystemReplicationQueue"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemReplicationQueue(const std::string & name_);
};

}
