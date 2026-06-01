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
        auto metadata_snapshot = inner_storage->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
        setInMemoryMetadata(*metadata_snapshot);
    }

    QueryProcessingStage::Enum StorageLoop::getQueryProcessingStage(
        ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
    {
        /// `LoopSource` always materialises the inner select with
        /// `QueryProcessingStage::Complete`, so the chunks it emits are plain column
        /// data. Delegating to `inner_storage` here could advertise `WithMergeableState`
        /// (e.g. when the inner storage is `Distributed`) and make the outer planner add
        /// a `MergingAggregatedStep`, which then trips on the missing chunk info — see
        /// issue #104863.
        return QueryProcessingStage::FetchColumns;
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

    void registerStorageLoop(StorageFactory & factory);
    void registerStorageLoop(StorageFactory & factory)
    {
        factory.registerStorage("Loop", [](const StorageFactory::Arguments & args)
        {
            StoragePtr inner_storage;
            return std::make_shared<StorageLoop>(args.table_id, inner_storage);
        });
    }
}
