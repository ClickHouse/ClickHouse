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
        ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
    {
        /// The Loop source emits raw rows: `LoopSource::initLoop` always builds the inner
        /// select via `InterpreterSelectQueryAnalyzer` / `InterpreterSelectWithUnionQuery`
        /// with `QueryProcessingStage::Complete`, so the chunks it produces are plain
        /// column data with no `AggregatedChunkInfo` attached.
        ///
        /// Previously this method delegated to `inner_storage->getQueryProcessingStage`,
        /// which for a wrapped Distributed/remote table (or any storage that can defer
        /// aggregation) could return `WithMergeableState`. The outer planner would then
        /// add a `MergingAggregatedStep` that asserts an `AggregatedChunkInfo` is present
        /// on every input chunk — and the LoopSource's plain chunks would trip the
        /// `LOGICAL_ERROR` "Chunk info was not set for chunk in MergingAggregatedTransform"
        /// (issue #104863). Without parallel replicas the same mismatch silently dropped
        /// the outer aggregation, returning duplicated rows instead.
        ///
        /// Reporting `FetchColumns` lets the outer planner add a regular `AggregatingStep`
        /// over the loop output, which matches what the source actually produces.
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

    void registerStorageLoop(StorageFactory & factory)
    {
        factory.registerStorage("Loop", [](const StorageFactory::Arguments & args)
        {
            StoragePtr inner_storage;
            return std::make_shared<StorageLoop>(args.table_id, inner_storage);
        });
    }
}
