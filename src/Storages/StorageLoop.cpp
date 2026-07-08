#include <Storages/StorageLoop.h>
#include <Storages/StorageFactory.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLoopStep.h>
#include <Common/CurrentThread.h>


namespace DB
{
    StorageLoop::StorageLoop(
            const StorageID & table_id_,
            StoragePtr inner_storage_,
            ASTPtr inner_table_function_ast_)
            : IStorage(table_id_)
            , inner_storage(std::move(inner_storage_))
            , inner_table_function_ast(std::move(inner_table_function_ast_))
    {
        setInMemoryMetadata(*inner_storage->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false));
    }

    QueryProcessingStage::Enum StorageLoop::getQueryProcessingStage(
        ContextPtr local_context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo & query_info) const
    {
        auto storage_snapshot = inner_storage->getStorageSnapshot(inner_storage->getInMemoryMetadataPtr(local_context, false), local_context);
        return inner_storage->getQueryProcessingStage(local_context, to_stage, storage_snapshot, query_info);
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
                column_names, query_info, storage_snapshot, context, processed_stage, inner_storage,
                inner_table_function_ast, max_block_size, num_streams
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
