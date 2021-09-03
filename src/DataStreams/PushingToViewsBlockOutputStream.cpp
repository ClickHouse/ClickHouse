#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageValues.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/checkStackSize.h>
#include <common/scope_guard.h>
#include "Processors/printPipeline.h"

#include <atomic>
#include <chrono>

namespace DB
{

struct ViewsData
{
    std::list<ViewRuntimeData> views;
    ContextPtr context;
    StorageID source_storage_id;
    StorageMetadataPtr source_metadata_snapshot;
    StoragePtr source_storage;

    /// In case of exception happened while inserting into main table, it is pushed to pipeline.
    /// Remember the first one, we should keep them after view processing.
    std::atomic_bool has_exception = false;
    std::exception_ptr first_exception;

    ViewsData(ContextPtr context_, StorageID source_storage_id_, StorageMetadataPtr source_metadata_snapshot_ , StoragePtr source_storage_)
        : context(std::move(context_))
        , source_storage_id(std::move(source_storage_id_))
        , source_metadata_snapshot(std::move(source_metadata_snapshot_))
        , source_storage(std::move(source_storage_))
    {
    }
};

using ViewsDataPtr = std::shared_ptr<ViewsData>;

class CopyingDataToViewsTransform final : public IProcessor
{
public:
    CopyingDataToViewsTransform(const Block & header, ViewsDataPtr data)
        : IProcessor({header}, OutputPorts(data->views.size(), header))
        , input(inputs.front())
        , views_data(std::move(data))
    {
        if (views_data->views.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CopyingDataToViewsTransform cannot have zero outputs");
    }

    String getName() const override { return "CopyingDataToViewsTransform"; }

    Status prepare() override
    {
        bool all_can_push = true;
        for (auto & output : outputs)
        {
            if (output.isFinished())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push data to view because output port is finished");

            if (!output.canPush())
                all_can_push = false;
        }

        if (!all_can_push)
            return Status::PortFull;

        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();

            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        auto data = input.pullData();
        if (data.exception)
        {
            if (!views_data->has_exception)
            {
                views_data->first_exception = data.exception;
                views_data->has_exception = true;
            }

            for (auto & output : outputs)
                output.pushException(data.exception);
        }
        else
        {
            for (auto & output : outputs)
                output.push(data.chunk.clone());
        }

        return Status::PortFull;
    }

    InputPort & getInputPort() { return input; }

private:
    InputPort & input;
    ViewsDataPtr views_data;
};

static void logQueryViews(std::list<ViewRuntimeData> & views, ContextPtr context);

class FinalizingViewsTransform final : public IProcessor
{
    struct ExceptionStatus
    {
        std::exception_ptr exception;
        bool is_first = false;
    };

    static InputPorts initPorts(std::vector<Block> headers)
    {
        InputPorts res;
        for (auto & header : headers)
            res.emplace_back(std::move(header));

        return res;
    }

public:
    FinalizingViewsTransform(std::vector<Block> headers, ViewsDataPtr data)
        : IProcessor(initPorts(std::move(headers)), {Block()})
        , output(outputs.front())
        , views_data(std::move(data))
    {
        statuses.resize(views_data->views.size());
    }

    String getName() const override { return "FinalizingViewsTransform"; }

    Status prepare() override
    {
        if (output.isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finalize views because output port is finished");

        if (!output.canPush())
            return Status::PortFull;

        size_t num_finished = 0;
        size_t pos = 0;
        for (auto & input : inputs)
        {
            auto i = pos;
            ++pos;

            if (input.isFinished())
            {
                ++num_finished;
                continue;
            }

            input.setNeeded();
            if (input.hasData())
            {
                auto data = input.pullData();
                if (data.exception)
                {
                    if (views_data->has_exception && views_data->first_exception == data.exception)
                        statuses[i].is_first = true;
                    else
                        statuses[i].exception = data.exception;

                    if (i == 0 && statuses[0].is_first)
                    {
                        output.pushData(std::move(data));
                        return Status::PortFull;
                    }
                }

                if (input.isFinished())
                    ++num_finished;
            }
        }

        if (num_finished == inputs.size())
        {
            if (!statuses.empty())
                return Status::Ready;

            output.finish();
            return Status::Finished;
        }

        return Status::NeedData;
    }

    void work() override
    {
        size_t i = 0;
        for (auto & view : views_data->views)
        {
            auto & status = statuses[i];
            ++i;

            if (status.exception)
            {
                if (!any_exception)
                    any_exception = status.exception;

                view.setException(std::move(status.exception));
            }
        }

        logQueryViews(views_data->views, views_data->context);

        statuses.clear();
    }

private:
    OutputPort & output;
    ViewsDataPtr views_data;
    std::vector<ExceptionStatus> statuses;
    std::exception_ptr any_exception;
};

Chain buildPushingToViewsDrain(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const ASTPtr & query_ptr,
    bool no_destination,
    std::vector<TableLockHolder> & locks,
    ExceptionKeepingTransformRuntimeDataPtr runtime_data)
{
    checkStackSize();

    /// If we don't write directly to the destination
    /// then expect that we're inserting with precalculated virtual columns
    auto storage_header = no_destination ? metadata_snapshot->getSampleBlockWithVirtuals(storage->getVirtuals())
                                         : metadata_snapshot->getSampleBlock();

    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    locks.emplace_back(storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout));

    /// If the "root" table deduplicates blocks, there are no need to make deduplication for children
    /// Moreover, deduplication for AggregatingMergeTree children could produce false positives due to low size of inserting blocks
    bool disable_deduplication_for_children = false;
    if (!context->getSettingsRef().deduplicate_blocks_in_dependent_materialized_views)
        disable_deduplication_for_children = !no_destination && storage->supportsDeduplication();

    auto table_id = storage->getStorageID();
    Dependencies dependencies = DatabaseCatalog::instance().getDependencies(table_id);

    /// We need special context for materialized views insertions
    ContextMutablePtr select_context;
    ContextMutablePtr insert_context;
    if (!dependencies.empty())
    {
        select_context = Context::createCopy(context);
        insert_context = Context::createCopy(context);

        const auto & insert_settings = insert_context->getSettingsRef();

        // Do not deduplicate insertions into MV if the main insertion is Ok
        if (disable_deduplication_for_children)
            insert_context->setSetting("insert_deduplicate", Field{false});

        // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
        if (insert_settings.min_insert_block_size_rows_for_materialized_views)
            insert_context->setSetting("min_insert_block_size_rows", insert_settings.min_insert_block_size_rows_for_materialized_views.value);
        if (insert_settings.min_insert_block_size_bytes_for_materialized_views)
            insert_context->setSetting("min_insert_block_size_bytes", insert_settings.min_insert_block_size_bytes_for_materialized_views.value);
    }

    std::vector<Chain> chains;
    auto views_data = std::make_shared<ViewsData>(context, table_id, metadata_snapshot, storage);

    for (const auto & database_table : dependencies)
    {
        auto dependent_table = DatabaseCatalog::instance().getTable(database_table, context);
        auto dependent_metadata_snapshot = dependent_table->getInMemoryMetadataPtr();

        ASTPtr query;
        Chain out;
        QueryViewsLogElement::ViewType type = QueryViewsLogElement::ViewType::DEFAULT;
        String target_name = database_table.getFullTableName();

        /// If the materialized view is executed outside of a query, for example as a result of SYSTEM FLUSH LOGS or
        /// SYSTEM FLUSH DISTRIBUTED ..., we can't attach to any thread group and we won't log, so there is no point on collecting metrics
        std::unique_ptr<ThreadStatus> thread_status = nullptr;

        ThreadGroupStatusPtr running_group = current_thread && current_thread->getThreadGroup()
            ? current_thread->getThreadGroup()
            : MainThreadStatus::getInstance().getThreadGroup();
        if (running_group)
        {
            /// We are creating a ThreadStatus per view to store its metrics individually
            /// Since calling ThreadStatus() changes current_thread we save it and restore it after the calls
            /// Later on, before doing any task related to a view, we'll switch to its ThreadStatus, do the work,
            /// and switch back to the original thread_status.
            auto * original_thread = current_thread;
            SCOPE_EXIT({ current_thread = original_thread; });

            thread_status = std::make_unique<ThreadStatus>();
            /// Disable query profiler for this ThreadStatus since the running (main query) thread should already have one
            /// If we didn't disable it, then we could end up with N + 1 (N = number of dependencies) profilers which means
            /// N times more interruptions
            thread_status->disableProfiling();
            thread_status->attachQuery(running_group);
        }

        auto view_runtime_data = std::make_shared<ExceptionKeepingTransformRuntimeData>(
            std::move(thread_status),
            database_table.getNameForLogs());

        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(dependent_table.get()))
        {
            type = QueryViewsLogElement::ViewType::MATERIALIZED;
            locks.emplace_back(materialized_view->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout));

            StoragePtr inner_table = materialized_view->getTargetTable();
            auto inner_table_id = inner_table->getStorageID();
            auto inner_metadata_snapshot = inner_table->getInMemoryMetadataPtr();
            query = dependent_metadata_snapshot->getSelectQuery().inner_query;
            target_name = inner_table_id.getFullTableName();

            std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
            insert->table_id = inner_table_id;

            /// Get list of columns we get from select query.
            auto header = InterpreterSelectQuery(query, select_context, SelectQueryOptions().analyze())
                .getSampleBlock();

            /// Insert only columns returned by select.
            auto list = std::make_shared<ASTExpressionList>();
            const auto & inner_table_columns = inner_metadata_snapshot->getColumns();
            for (const auto & column : header)
            {
                /// But skip columns which storage doesn't have.
                if (inner_table_columns.hasPhysical(column.name))
                    list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
            }

            insert->columns = std::move(list);

            ASTPtr insert_query_ptr(insert.release());
            InterpreterInsertQuery interpreter(insert_query_ptr, insert_context, false, false, false, runtime_data);
            BlockIO io = interpreter.execute();
            out = std::move(io.out);
        }
        else if (const auto * live_view = dynamic_cast<const StorageLiveView *>(dependent_table.get()))
        {
            type = QueryViewsLogElement::ViewType::LIVE;
            query = live_view->getInnerQuery(); // Used only to log in system.query_views_log
            out = buildPushingToViewsDrain(
                dependent_table, dependent_metadata_snapshot, insert_context, ASTPtr(), true, locks, view_runtime_data);
        }
        else
            out = buildPushingToViewsDrain(
                dependent_table, dependent_metadata_snapshot, insert_context, ASTPtr(), false, locks, view_runtime_data);

        QueryViewsLogElement::ViewRuntimeStats runtime_stats{
            target_name,
            type,
            std::move(thread_status),
            0,
            std::chrono::system_clock::now(),
            QueryViewsLogElement::ViewStatus::EXCEPTION_BEFORE_START};

        views_data->views.emplace_back(ViewRuntimeData{
            std::move(query),
            out.getInputHeader(),
            database_table,
            dependent_table,
            dependent_metadata_snapshot,
            select_context,
            nullptr,
            std::move(runtime_stats)});

        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform>(
            storage_header, views_data->views.back(), views_data->source_storage_id, views_data->source_metadata_snapshot, views_data->source_storage);
        executing_inner_query->setRuntimeData(view_runtime_data);

        out.addSource(std::move(executing_inner_query));
        chains.emplace_back(std::move(out));

        /// Add the view to the query access info so it can appear in system.query_log
        if (!no_destination)
        {
            context->getQueryContext()->addQueryAccessInfo(
                backQuoteIfNeed(database_table.getDatabaseName()), target_name, {}, "", database_table.getFullTableName());
        }
    }

    Chain result_chain;

    size_t num_views = views_data->views.size();
    if (num_views != 0)
    {
        std::vector<Block> headers;
        headers.reserve(num_views);
        for (const auto & chain : chains)
            headers.push_back(chain.getOutputHeader());

        auto copying_data = std::make_shared<CopyingDataToViewsTransform>(storage_header, views_data);
        auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(headers), views_data);
        auto out = copying_data->getOutputs().begin();
        auto in = finalizing_views->getInputs().begin();

        std::list<ProcessorPtr> processors;

        for (auto & chain : chains)
        {
            connect(*out, chain.getInputPort());
            connect(chain.getOutputPort(), *in);
            ++in;
            ++out;
            processors.splice(processors.end(), Chain::getProcessors(std::move(chain)));
        }

        processors.emplace_front(std::move(copying_data));
        processors.emplace_back(std::move(finalizing_views));
        result_chain = Chain(std::move(processors));
    }

    /// Do not push to destination table if the flag is set
    if (!no_destination)
    {
        auto sink = storage->write(query_ptr, storage->getInMemoryMetadataPtr(), context);
        sink->setRuntimeData(runtime_data);
        metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        result_chain.addSource(std::move(sink));
    }

    /// TODO: add pushing to live view
    if (result_chain.empty())
        result_chain.addSink(std::make_shared<NullSinkToStorage>(storage_header));

    return result_chain;
}

static void process(Block & block, ViewRuntimeData & view, const StorageID & source_storage_id, const StorageMetadataPtr & source_metadata_snapshot, const StoragePtr & source_storage)
{
    const auto & context = view.context;

    /// We create a table with the same name as original table and the same alias columns,
    ///  but it will contain single block (that is INSERT-ed into main table).
    /// InterpreterSelectQuery will do processing of alias columns.
    auto local_context = Context::createCopy(context);
    local_context->addViewSource(StorageValues::create(
        source_storage_id,
        source_metadata_snapshot->getColumns(),
        block,
        source_storage->getVirtuals()));

    /// We need keep InterpreterSelectQuery, until the processing will be finished, since:
    ///
    /// - We copy Context inside InterpreterSelectQuery to support
    ///   modification of context (Settings) for subqueries
    /// - InterpreterSelectQuery lives shorter than query pipeline.
    ///   It's used just to build the query pipeline and no longer needed
    /// - ExpressionAnalyzer and then, Functions, that created in InterpreterSelectQuery,
    ///   **can** take a reference to Context from InterpreterSelectQuery
    ///   (the problem raises only when function uses context from the
    ///    execute*() method, like FunctionDictGet do)
    /// - These objects live inside query pipeline (DataStreams) and the reference become dangling.
    InterpreterSelectQuery select(view.query, local_context, SelectQueryOptions());

    auto io = select.execute();
    io.pipeline.resize(1);

    /// Squashing is needed here because the materialized view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    io.pipeline.addTransform(std::make_shared<SquashingChunksTransform>(
        io.pipeline.getHeader(),
        context->getSettingsRef().min_insert_block_size_rows,
        context->getSettingsRef().min_insert_block_size_bytes));

    auto converting = ActionsDAG::makeConvertingActions(
        io.pipeline.getHeader().getColumnsWithTypeAndName(),
        view.sample_block.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    io.pipeline.addTransform(std::make_shared<ExpressionTransform>(
        io.pipeline.getHeader(),
        std::make_shared<ExpressionActions>(std::move(converting))));

    io.pipeline.setProgressCallback([context](const Progress & progress)
    {
        CurrentThread::updateProgressIn(progress);
        if (auto callback = context->getProgressCallback())
            callback(progress);
    });


    PullingPipelineExecutor executor(io.pipeline);
    if (!executor.pull(block))
    {
        block.clear();
        return;
    }

    WriteBufferFromOwnString buf;
    auto pipe = QueryPipeline::getPipe(std::move(io.pipeline));
    const auto & processors = pipe.getProcessors();
    printPipeline(processors, buf);

    if (executor.pull(block))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Single chunk is expected from view inner query {}", view.query);
}

void ExecutingInnerQueryFromViewTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    process(block, view, source_storage_id, source_metadata_snapshot, source_storage);
    chunk.setColumns(block.getColumns(), block.rows());
}

static void logQueryViews(std::list<ViewRuntimeData> & views, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    const UInt64 min_query_duration = settings.log_queries_min_query_duration_ms.totalMilliseconds();
    const QueryViewsLogElement::ViewStatus min_status = settings.log_queries_min_type;
    if (views.empty() || !settings.log_queries || !settings.log_query_views)
        return;

    for (auto & view : views)
    {
        if ((min_query_duration && view.runtime_stats.elapsed_ms <= min_query_duration) || (view.runtime_stats.event_status < min_status))
            continue;

        try
        {
            if (view.runtime_stats.thread_status)
                view.runtime_stats.thread_status->logToQueryViewsLog(view);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
