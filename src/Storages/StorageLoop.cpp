#include <Storages/StorageLoop.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLoopStep.h>
#include <Common/CurrentThread.h>


namespace DB
{
    StorageLoop::StorageLoop(
            const StorageID & table_id_,
            const StoragePtr & inner_storage_,
            ASTPtr inner_table_function_ast_)
            : IStorage(table_id_)
            , inner_table_id(inner_storage_->getStorageID())
            , inner_table_function_ast(std::move(inner_table_function_ast_))
    {
        /// The source is followed by name and resolved per read: holding its `StoragePtr` would keep
        /// the storage object alive after the source is dropped, and a pinned UUID would not survive
        /// dropping and recreating it.
        inner_table_id.uuid = UUIDHelpers::Nil;

        auto metadata_snapshot = inner_storage_->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
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
            QueryProcessingStage::Enum,
            size_t,
            size_t)
    {
        query_info.optimize_trivial_count = false;

        StoragePtr inner_storage;
        if (!inner_table_function_ast)
            inner_storage = DatabaseCatalog::instance().getTable(inner_table_id, context);

        query_plan.addStep(std::make_unique<ReadFromLoopStep>(
                column_names, query_info, storage_snapshot, context, std::move(inner_storage),
                inner_table_function_ast
        ));
    }

    void registerStorageLoop(StorageFactory & factory);
    void registerStorageLoop(StorageFactory & factory)
    {
        factory.registerStorage("Loop", [](const StorageFactory::Arguments & args)
        {
            StoragePtr inner_storage;
            return std::make_shared<StorageLoop>(args.table_id, inner_storage);
        },
        {},
        Documentation{
            .description = "Reads from an inner table or table function repeatedly, returning its rows in an infinite loop. "
                "It is the backing engine for the `loop` table function and is mainly useful for testing and generating continuous streams of data.",
            .syntax = "SELECT * FROM loop(database, table)"});
    }
}
