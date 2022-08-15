#include <Processors/Transforms/buildPushingToViewsChain.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageValues.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <atomic>
#include <chrono>

namespace ProfileEvents
{
    extern const Event SelectedBytes;
    extern const Event SelectedRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct ViewsData
{
    /// Separate information for every view.
    std::list<ViewRuntimeData> views;
    /// Some common info about source storage.
    ContextPtr context;
    StorageID source_storage_id;
    StorageMetadataPtr source_metadata_snapshot;
    StoragePtr source_storage;
    /// This value is actually only for logs.
    size_t max_threads = 1;

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

/// Copies data inserted into table for every dependent table.
class CopyingDataToViewsTransform final : public IProcessor
{
public:
    CopyingDataToViewsTransform(const Block & header, ViewsDataPtr data);

    String getName() const override { return "CopyingDataToViewsTransform"; }
    Status prepare() override;
    InputPort & getInputPort() { return input; }

private:
    InputPort & input;
    ViewsDataPtr views_data;
};

/// For source chunk, execute view query over it.
class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(const Block & header, ViewRuntimeData & view_, ViewsDataPtr views_data_);

    String getName() const override { return "ExecutingInnerQueryFromView"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

private:
    ViewsDataPtr views_data;
    ViewRuntimeData & view;

    struct State
    {
        QueryPipeline pipeline;
        PullingPipelineExecutor executor;

        explicit State(QueryPipeline pipeline_)
            : pipeline(std::move(pipeline_))
            , executor(pipeline)
        {
        }
    };

    std::optional<State> state;
};

/// Insert into LiveView.
class PushingToLiveViewSink final : public SinkToStorage
{
public:
    PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, StoragePtr storage_holder_, ContextPtr context_);
    String getName() const override { return "PushingToLiveViewSink"; }
    void consume(Chunk chunk) override;

private:
    StorageLiveView & live_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

/// Insert into WindowView.
class PushingToWindowViewSink final : public SinkToStorage
{
public:
    PushingToWindowViewSink(const Block & header, StorageWindowView & window_view_, StoragePtr storage_holder_, ContextPtr context_);
    String getName() const override { return "PushingToWindowViewSink"; }
    void consume(Chunk chunk) override;

private:
    StorageWindowView & window_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

/// For every view, collect exception.
/// Has single output with empty header.
/// If any exception happen before view processing, pass it.
/// Othervise return any exception from any view.
class FinalizingViewsTransform final : public IProcessor
{
    struct ExceptionStatus
    {
        std::exception_ptr exception;
        bool is_first = false;
    };

    static InputPorts initPorts(std::vector<Block> headers);

public:
    FinalizingViewsTransform(std::vector<Block> headers, ViewsDataPtr data);

    String getName() const override { return "FinalizingViewsTransform"; }
    Status prepare() override;
    void work() override;

private:
    OutputPort & output;
    ViewsDataPtr views_data;
    std::vector<ExceptionStatus> statuses;
    std::exception_ptr any_exception;
};


Chain buildPushingToViewsChain(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const ASTPtr & query_ptr,
    bool no_destination,
    ThreadStatus * thread_status,
    std::atomic_uint64_t * elapsed_counter_ms,
    const Block & live_view_header)
{
    checkStackSize();
    Chain result_chain;

    /// If we don't write directly to the destination
    /// then expect that we're inserting with precalculated virtual columns
    auto storage_header = no_destination ? metadata_snapshot->getSampleBlockWithVirtuals(storage->getVirtuals())
                                         : metadata_snapshot->getSampleBlock();

    /** TODO This is a very important line. At any insertion into the table one of chains should own lock.
      * Although now any insertion into the table is done via PushingToViews chain,
      *  but it's clear that here is not the best place for this functionality.
      */
    result_chain.addTableLock(storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout));

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
    ViewsDataPtr views_data;
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

        views_data = std::make_shared<ViewsData>(select_context, table_id, metadata_snapshot, storage);
    }

    std::vector<Chain> chains;

    for (const auto & database_table : dependencies)
    {
        auto dependent_table = DatabaseCatalog::instance().getTable(database_table, context);
        auto dependent_metadata_snapshot = dependent_table->getInMemoryMetadataPtr();

        ASTPtr query;
        Chain out;

        /// If the materialized view is executed outside of a query, for example as a result of SYSTEM FLUSH LOGS or
        /// SYSTEM FLUSH DISTRIBUTED ..., we can't attach to any thread group and we won't log, so there is no point on collecting metrics
        std::unique_ptr<ThreadStatus> view_thread_status_ptr = nullptr;

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

            view_thread_status_ptr = std::make_unique<ThreadStatus>();
            /// Disable query profiler for this ThreadStatus since the running (main query) thread should already have one
            /// If we didn't disable it, then we could end up with N + 1 (N = number of dependencies) profilers which means
            /// N times more interruptions
            view_thread_status_ptr->disableProfiling();
            view_thread_status_ptr->attachQuery(running_group);
        }

        auto runtime_stats = std::make_unique<QueryViewsLogElement::ViewRuntimeStats>();
        runtime_stats->target_name = database_table.getFullTableName();
        runtime_stats->thread_status = std::move(view_thread_status_ptr);
        runtime_stats->event_time = std::chrono::system_clock::now();
        runtime_stats->event_status = QueryViewsLogElement::ViewStatus::EXCEPTION_BEFORE_START;

        auto & type = runtime_stats->type;
        auto & target_name = runtime_stats->target_name;
        auto * view_thread_status = runtime_stats->thread_status.get();
        auto * view_counter_ms = &runtime_stats->elapsed_ms;

        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(dependent_table.get()))
        {
            type = QueryViewsLogElement::ViewType::MATERIALIZED;
            result_chain.addTableLock(materialized_view->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout));

            StoragePtr inner_table = materialized_view->getTargetTable();
            auto inner_table_id = inner_table->getStorageID();
            auto inner_metadata_snapshot = inner_table->getInMemoryMetadataPtr();
            query = dependent_metadata_snapshot->getSelectQuery().inner_query;
            target_name = inner_table_id.getFullTableName();

            /// Get list of columns we get from select query.
            auto header = InterpreterSelectQuery(query, select_context, SelectQueryOptions().analyze())
                .getSampleBlock();

            /// Insert only columns returned by select.
            Names insert_columns;
            const auto & inner_table_columns = inner_metadata_snapshot->getColumns();
            for (const auto & column : header)
            {
                /// But skip columns which storage doesn't have.
                if (inner_table_columns.hasPhysical(column.name))
                    insert_columns.emplace_back(column.name);
            }

            InterpreterInsertQuery interpreter(nullptr, insert_context, false, false, false);
            out = interpreter.buildChain(inner_table, inner_metadata_snapshot, insert_columns, view_thread_status, view_counter_ms);
            out.addStorageHolder(dependent_table);
            out.addStorageHolder(inner_table);
        }
        else if (auto * live_view = dynamic_cast<StorageLiveView *>(dependent_table.get()))
        {
            runtime_stats->type = QueryViewsLogElement::ViewType::LIVE;
            query = live_view->getInnerQuery(); // Used only to log in system.query_views_log
            out = buildPushingToViewsChain(
                dependent_table, dependent_metadata_snapshot, insert_context, ASTPtr(), true, view_thread_status, view_counter_ms, storage_header);
        }
        else if (auto * window_view = dynamic_cast<StorageWindowView *>(dependent_table.get()))
        {
            runtime_stats->type = QueryViewsLogElement::ViewType::WINDOW;
            query = window_view->getMergeableQuery(); // Used only to log in system.query_views_log
            out = buildPushingToViewsChain(
                dependent_table, dependent_metadata_snapshot, insert_context, ASTPtr(), true, view_thread_status, view_counter_ms);
        }
        else
            out = buildPushingToViewsChain(
                dependent_table, dependent_metadata_snapshot, insert_context, ASTPtr(), false, view_thread_status, view_counter_ms);

        views_data->views.emplace_back(ViewRuntimeData{ //-V614
            std::move(query),
            out.getInputHeader(),
            database_table,
            nullptr,
            std::move(runtime_stats)});

        if (type == QueryViewsLogElement::ViewType::MATERIALIZED)
        {
            auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform>(
                storage_header, views_data->views.back(), views_data);
            executing_inner_query->setRuntimeData(view_thread_status, view_counter_ms);

            out.addSource(std::move(executing_inner_query));
        }

        chains.emplace_back(std::move(out));

        /// Add the view to the query access info so it can appear in system.query_log
        if (!no_destination)
        {
            context->getQueryContext()->addQueryAccessInfo(
                backQuoteIfNeed(database_table.getDatabaseName()), views_data->views.back().runtime_stats->target_name, {}, "", database_table.getFullTableName());
        }
    }

    if (views_data)
    {
        size_t num_views = views_data->views.size();
        const Settings & settings = context->getSettingsRef();
        if (settings.parallel_view_processing)
            views_data->max_threads = settings.max_threads ? std::min(static_cast<size_t>(settings.max_threads), num_views) : num_views;

        std::vector<Block> headers;
        headers.reserve(num_views);
        for (const auto & chain : chains)
            headers.push_back(chain.getOutputHeader());

        auto copying_data = std::make_shared<CopyingDataToViewsTransform>(storage_header, views_data);
        auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(headers), views_data);
        auto out = copying_data->getOutputs().begin();
        auto in = finalizing_views->getInputs().begin();

        size_t max_parallel_streams = 0;

        std::list<ProcessorPtr> processors;

        for (auto & chain : chains)
        {
            max_parallel_streams += std::max<size_t>(chain.getNumThreads(), 1);
            result_chain.attachResources(chain.detachResources());
            connect(*out, chain.getInputPort());
            connect(chain.getOutputPort(), *in);
            ++in;
            ++out;
            processors.splice(processors.end(), Chain::getProcessors(std::move(chain)));
        }

        processors.emplace_front(std::move(copying_data));
        processors.emplace_back(std::move(finalizing_views));
        result_chain = Chain(std::move(processors));
        result_chain.setNumThreads(std::min(views_data->max_threads, max_parallel_streams));
    }

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        auto sink = std::make_shared<PushingToLiveViewSink>(live_view_header, *live_view, storage, context);
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));
    }
    else if (auto * window_view = dynamic_cast<StorageWindowView *>(storage.get()))
    {
        auto sink = std::make_shared<PushingToWindowViewSink>(window_view->getInputHeader(), *window_view, storage, context);
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));
    }
    /// Do not push to destination table if the flag is set
    else if (!no_destination)
    {
        auto sink = storage->write(query_ptr, metadata_snapshot, context);
        metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        result_chain.addSource(std::move(sink));
    }

    /// TODO: add pushing to live view
    if (result_chain.empty())
        result_chain.addSink(std::make_shared<NullSinkToStorage>(storage_header));

    if (result_chain.getOutputHeader().columns() != 0)
    {
        /// Convert result header to empty block.
        auto dag = ActionsDAG::makeConvertingActions(result_chain.getOutputHeader().getColumnsWithTypeAndName(), {}, ActionsDAG::MatchColumnsMode::Name);
        auto actions = std::make_shared<ExpressionActions>(std::move(dag));
        result_chain.addSink(std::make_shared<ConvertingTransform>(result_chain.getOutputHeader(), std::move(actions)));
    }

    return result_chain;
}

static QueryPipeline process(Block block, ViewRuntimeData & view, const ViewsData & views_data)
{
    const auto & context = views_data.context;

    /// We create a table with the same name as original table and the same alias columns,
    ///  but it will contain single block (that is INSERT-ed into main table).
    /// InterpreterSelectQuery will do processing of alias columns.
    auto local_context = Context::createCopy(context);
    local_context->addViewSource(std::make_shared<StorageValues>(
        views_data.source_storage_id,
        views_data.source_metadata_snapshot->getColumns(),
        std::move(block),
        views_data.source_storage->getVirtuals()));

    InterpreterSelectQuery select(view.query, local_context, SelectQueryOptions());
    auto pipeline = select.buildQueryPipeline();
    pipeline.resize(1);
    pipeline.dropTotalsAndExtremes();

    /// Squashing is needed here because the materialized view query can generate a lot of blocks
    /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
    /// and two-level aggregation is triggered).
    pipeline.addTransform(std::make_shared<SquashingChunksTransform>(
        pipeline.getHeader(),
        context->getSettingsRef().min_insert_block_size_rows,
        context->getSettingsRef().min_insert_block_size_bytes));

    auto converting = ActionsDAG::makeConvertingActions(
        pipeline.getHeader().getColumnsWithTypeAndName(),
        view.sample_block.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    pipeline.addTransform(std::make_shared<ExpressionTransform>(
        pipeline.getHeader(),
        std::make_shared<ExpressionActions>(std::move(converting))));

    return QueryPipelineBuilder::getPipeline(std::move(pipeline));
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
        const auto & stats = *view.runtime_stats;
        if ((min_query_duration && stats.elapsed_ms <= min_query_duration) || (stats.event_status < min_status))
            continue;

        try
        {
            if (stats.thread_status)
                stats.thread_status->logToQueryViewsLog(view);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


CopyingDataToViewsTransform::CopyingDataToViewsTransform(const Block & header, ViewsDataPtr data)
    : IProcessor({header}, OutputPorts(data->views.size(), header))
    , input(inputs.front())
    , views_data(std::move(data))
{
    if (views_data->views.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CopyingDataToViewsTransform cannot have zero outputs");
}

IProcessor::Status CopyingDataToViewsTransform::prepare()
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


ExecutingInnerQueryFromViewTransform::ExecutingInnerQueryFromViewTransform(
    const Block & header,
    ViewRuntimeData & view_,
    std::shared_ptr<ViewsData> views_data_)
    : ExceptionKeepingTransform(header, view_.sample_block)
    , views_data(std::move(views_data_))
    , view(view_)
{
}

void ExecutingInnerQueryFromViewTransform::onConsume(Chunk chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    state.emplace(process(block, view, *views_data));
}


ExecutingInnerQueryFromViewTransform::GenerateResult ExecutingInnerQueryFromViewTransform::onGenerate()
{
    GenerateResult res;
    if (!state.has_value())
        return res;

    res.is_done = false;
    while (!res.is_done)
    {
        res.is_done = !state->executor.pull(res.chunk);
        if (res.chunk)
            break;
    }

    if (res.is_done)
        state.reset();

    return res;
}

PushingToLiveViewSink::PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, StoragePtr storage_holder_, ContextPtr context_)
    : SinkToStorage(header)
    , live_view(live_view_)
    , storage_holder(std::move(storage_holder_))
    , context(std::move(context_))
{
}

void PushingToLiveViewSink::consume(Chunk chunk)
{
    Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
    StorageLiveView::writeIntoLiveView(live_view, getHeader().cloneWithColumns(chunk.detachColumns()), context);
    auto * process = context->getProcessListElement();
    if (process)
        process->updateProgressIn(local_progress);
    ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
}


PushingToWindowViewSink::PushingToWindowViewSink(
    const Block & header, StorageWindowView & window_view_,
    StoragePtr storage_holder_, ContextPtr context_)
    : SinkToStorage(header)
    , window_view(window_view_)
    , storage_holder(std::move(storage_holder_))
    , context(std::move(context_))
{
}

void PushingToWindowViewSink::consume(Chunk chunk)
{
    Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
    StorageWindowView::writeIntoWindowView(
        window_view, getHeader().cloneWithColumns(chunk.detachColumns()), context);
    auto * process = context->getProcessListElement();
    if (process)
        process->updateProgressIn(local_progress);
    ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
}


FinalizingViewsTransform::FinalizingViewsTransform(std::vector<Block> headers, ViewsDataPtr data)
    : IProcessor(initPorts(std::move(headers)), {Block()})
    , output(outputs.front())
    , views_data(std::move(data))
{
    statuses.resize(views_data->views.size());
}

InputPorts FinalizingViewsTransform::initPorts(std::vector<Block> headers)
{
    InputPorts res;
    for (auto & header : headers)
        res.emplace_back(std::move(header));

    return res;
}

IProcessor::Status FinalizingViewsTransform::prepare()
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
            //std::cerr << "********** FinalizingViewsTransform got input " << i << " has exc " << bool(data.exception) << std::endl;
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

        if (any_exception)
            output.pushException(any_exception);

        output.finish();
        return Status::Finished;
    }

    return Status::NeedData;
}

static std::exception_ptr addStorageToException(std::exception_ptr ptr, const StorageID & storage)
{
    try
    {
        std::rethrow_exception(ptr);
    }
    catch (DB::Exception & exception)
    {
        exception.addMessage("while pushing to view {}", storage.getNameForLogs());
        return std::current_exception();
    }
    catch (...)
    {
        return std::current_exception();
    }

    __builtin_unreachable();
}

void FinalizingViewsTransform::work()
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

            view.setException(addStorageToException(status.exception, view.table_id));
        }
        else
        {
            view.runtime_stats->setStatus(QueryViewsLogElement::ViewStatus::QUERY_FINISH);

            LOG_TRACE(
                &Poco::Logger::get("PushingToViews"),
                "Pushing ({}) from {} to {} took {} ms.",
                views_data->max_threads <= 1 ? "sequentially" : ("parallel " + std::to_string(views_data->max_threads)),
                views_data->source_storage_id.getNameForLogs(),
                view.table_id.getNameForLogs(),
                view.runtime_stats->elapsed_ms);
        }
    }

    logQueryViews(views_data->views, views_data->context);

    statuses.clear();
}

}
