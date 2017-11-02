#include "PushingToViewsBlockOutputStream.h"
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>


namespace DB
{

PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(
        String database, String table, StoragePtr storage,
        const Context & context_, const ASTPtr & query_ptr_, bool no_destination)
    : context(context_), query_ptr(query_ptr_)
{
    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(storage->lockStructure(true, __PRETTY_FUNCTION__));

    if (!table.empty())
    {
        Dependencies dependencies = context.getDependencies(database, table);

        /// We need special context for materialized views insertions
        if (!dependencies.empty())
        {
            views_context = std::make_unique<Context>(context);
            // Do not deduplicate insertions into MV if the main insertion is Ok
            views_context->getSettingsRef().insert_deduplicate = false;
        }

        for (const auto & database_table : dependencies)
        {
            auto dependent_table = context.getTable(database_table.first, database_table.second);
            auto & materialized_view = dynamic_cast<const StorageMaterializedView &>(*dependent_table);

            auto query = materialized_view.getInnerQuery();
            auto next = std::make_shared<PushingToViewsBlockOutputStream>(
                    database_table.first, database_table.second, dependent_table, *views_context, ASTPtr());

            views.emplace_back(std::move(query), std::move(next));
        }
    }

    /* Do not push to destination table if the flag is set */
    if (!no_destination)
    {
        output = storage->write(query_ptr, context.getSettingsRef());
        replicated_output = dynamic_cast<ReplicatedMergeTreeBlockOutputStream *>(output.get());
    }
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    if (output)
        output->write(block);

    /// Don't process materialized views if this block is duplicate
    if (replicated_output && replicated_output->lastBlockIsDuplicate())
        return;

    /// Insert data into materialized views only after successful insert into main table
    for (auto & view : views)
    {
        BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
        InterpreterSelectQuery select(view.first, *views_context, QueryProcessingStage::Complete, 0, from);
        BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
        copyData(*data, *view.second);
    }
}

}
