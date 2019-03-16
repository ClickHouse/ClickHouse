#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>

namespace DB
{

PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(
    const String & database, const String & table, const StoragePtr & storage_,
    const Context & context_, const ASTPtr & query_ptr_, bool no_destination)
    : storage(storage_), context(context_), query_ptr(query_ptr_)
{
    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(storage->lockStructureForShare(true, context.getCurrentQueryId()));

    /// If the "root" table deduplactes blocks, there are no need to make deduplication for children
    /// Moreover, deduplication for AggregatingMergeTree children could produce false positives due to low size of inserting blocks
    bool disable_deduplication_for_children = !no_destination && storage->supportsDeduplication();

    if (!table.empty())
    {
        Dependencies dependencies = context.getDependencies(database, table);

        /// We need special context for materialized views insertions
        if (!dependencies.empty())
        {
            views_context = std::make_unique<Context>(context);
            // Do not deduplicate insertions into MV if the main insertion is Ok
            if (disable_deduplication_for_children)
                views_context->getSettingsRef().insert_deduplicate = false;
        }

        for (const auto & database_table : dependencies)
        {
            auto dependent_table = context.getTable(database_table.first, database_table.second);
            auto & materialized_view = dynamic_cast<const StorageMaterializedView &>(*dependent_table);

            if (StoragePtr inner_table = materialized_view.tryGetTargetTable())
                addTableLock(inner_table->lockStructureForShare(true, context.getCurrentQueryId()));

            auto query = materialized_view.getInnerQuery();
            BlockOutputStreamPtr out = std::make_shared<PushingToViewsBlockOutputStream>(
                database_table.first, database_table.second, dependent_table, *views_context, ASTPtr());
            views.emplace_back(ViewInfo{std::move(query), database_table.first, database_table.second, std::move(out)});
        }
    }

    /* Do not push to destination table if the flag is set */
    if (!no_destination)
    {
        output = storage->write(query_ptr, context);
        replicated_output = dynamic_cast<ReplicatedMergeTreeBlockOutputStream *>(output.get());
    }
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(block);

    if (output)
        output->write(block);

    /// Don't process materialized views if this block is duplicate
    if (replicated_output && replicated_output->lastBlockIsDuplicate())
        return;

    // Insert data into materialized views only after successful insert into main table
    const Settings & settings = context.getSettingsRef();
    if (settings.parallel_view_processing && views.size() > 1)
    {
        // Push to views concurrently if enabled, and more than one view is attached
        ThreadPool pool(std::min(size_t(settings.max_threads), views.size()));
        for (size_t view_num = 0; view_num < views.size(); ++view_num)
        {
            auto thread_group = CurrentThread::getGroup();
            pool.schedule([=]
            {
                setThreadName("PushingToViews");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                process(block, view_num);
            });
        }
        // Wait for concurrent view processing
        pool.wait();
    }
    else
    {
        // Process sequentially
        for (size_t view_num = 0; view_num < views.size(); ++view_num)
            process(block, view_num);
    }
}

void PushingToViewsBlockOutputStream::writePrefix()
{
    if (output)
        output->writePrefix();

    for (auto & view : views)
    {
        try
        {
            view.out->writePrefix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.database + "." + view.table);
            throw;
        }
    }
}

void PushingToViewsBlockOutputStream::writeSuffix()
{
    if (output)
        output->writeSuffix();

    for (auto & view : views)
    {
        try
        {
            view.out->writeSuffix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.database + "." + view.table);
            throw;
        }
    }
}

void PushingToViewsBlockOutputStream::flush()
{
    if (output)
        output->flush();

    for (auto & view : views)
        view.out->flush();
}

void PushingToViewsBlockOutputStream::process(const Block & block, size_t view_num)
{
    auto & view = views[view_num];

    try
    {
        BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
        InterpreterSelectQuery select(view.query, *views_context, from);
        BlockInputStreamPtr in = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
        /// Squashing is needed here because the materialized view query can generate a lot of blocks
        /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
        /// and two-level aggregation is triggered).
        in = std::make_shared<SquashingBlockInputStream>(
            in, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);

        in->readPrefix();

        while (Block result_block = in->read())
        {
            Nested::validateArraySizes(result_block);
            view.out->write(result_block);
        }

        in->readSuffix();
    }
    catch (Exception & ex)
    {
        ex.addMessage("while pushing to view " + backQuoteIfNeed(view.database) + "." + backQuoteIfNeed(view.table));
        throw;
    }
}

}
