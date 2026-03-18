#include "StorageLoop.h"
#include <Storages/StorageFactory.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLoopStep.h>


namespace DB
{
    namespace ErrorCodes
    {

    }
    StorageLoop::StorageLoop(
            const StorageID & table_id_,
            StoragePtr inner_storage_)
            : IStorage(table_id_)
            , inner_storage(std::move(inner_storage_))
    {
        StorageInMemoryMetadata storage_metadata = inner_storage->getInMemoryMetadata();
        setInMemoryMetadata(storage_metadata);
    }


    void StorageLoop::read(
            QueryPlan & query_plan,
            const Names & column_names,
            const StorageSnapshotPtr & storage_snapshot,
            SelectQueryInfo & query_info,
            ContextPtr context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            size_t num_streams)
    {
        query_info.optimize_trivial_count = false;

        query_plan.addStep(std::make_unique<ReadFromLoopStep>(
                column_names, query_info, storage_snapshot, context, processed_stage, inner_storage, max_block_size, num_streams
        ));
    }

    void registerStorageLoop(StorageFactory & factory)
    {
        factory.registerStorage("Loop", [](const StorageFactory::Arguments & args)
        {
            StoragePtr inner_storage;
            return std::make_shared<StorageLoop>(args.table_id, inner_storage);
        });
    }
}
