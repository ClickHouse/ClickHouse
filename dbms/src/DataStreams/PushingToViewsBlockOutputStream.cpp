#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/StorageValues.h>
#include <Storages/LiveView/StorageLiveView.h>

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

            ASTPtr query;
            BlockOutputStreamPtr out;

            if (auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(dependent_table.get()))
            {
                StoragePtr inner_table = materialized_view->getTargetTable();
                query = materialized_view->getInnerQuery();
                std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
                insert->database = inner_table->getDatabaseName();
                insert->table = inner_table->getTableName();
                ASTPtr insert_query_ptr(insert.release());
                InterpreterInsertQuery interpreter(insert_query_ptr, *views_context);
                BlockIO io = interpreter.execute();
                out = io.out;
            }
            else if (dynamic_cast<const StorageLiveView *>(dependent_table.get()))
                out = std::make_shared<PushingToViewsBlockOutputStream>(
                        database_table.first, database_table.second, dependent_table, *views_context, ASTPtr(), true);
            else
                out = std::make_shared<PushingToViewsBlockOutputStream>(
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


Block PushingToViewsBlockOutputStream::getHeader() const
{
    /// If we don't write directly to the destination
    /// then expect that we're inserting with precalculated virtual columns
    if (output)
        return storage->getSampleBlock();
    else
        return storage->getSampleBlockWithVirtuals();
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(block);

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        StorageLiveView::writeIntoLiveView(*live_view, block, context);
    }
    else
    {
        if (output)
            /// TODO: to support virtual and alias columns inside MVs, we should return here the inserted block extended
            ///       with additional columns directly from storage and pass it to MVs instead of raw block.
            output->write(block);
    }

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
        BlockInputStreamPtr in;

        if (view.query)
        {
            /// We create a table with the same name as original table and the same alias columns,
            ///  but it will contain single block (that is INSERT-ed into main table).
            /// InterpreterSelectQuery will do processing of alias columns.
            Context local_context = *views_context;
            local_context.addViewSource(
                    StorageValues::create(storage->getDatabaseName(), storage->getTableName(), storage->getColumns(),
                                          block));
            InterpreterSelectQuery select(view.query, local_context, SelectQueryOptions());
            in = std::make_shared<MaterializingBlockInputStream>(select.execute().in);

            /// Squashing is needed here because the materialized view query can generate a lot of blocks
            /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
            /// and two-level aggregation is triggered).
            in = std::make_shared<SquashingBlockInputStream>(
                    in, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
            in = std::make_shared<ConvertingBlockInputStream>(context, in, view.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Name);
        }
        else
            in = std::make_shared<OneBlockInputStream>(block);

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
