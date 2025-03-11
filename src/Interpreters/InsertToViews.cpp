#include <Interpreters/InsertToViews.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageValues.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/CheckConstraintsTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/NestedElementsValidationTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <IO/Progress.h>

#include <Core/Settings.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include "Interpreters/StorageID.h"
#include "QueryPipeline/Chain.h"
#include "base/defines.h"

namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
    extern const Event InsertQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
}

namespace fmt
{
    template <>
    struct formatter<DB::ViewsManager::StorageIDPrivate>
    {
        static constexpr auto parse(format_parse_context & ctx)
        {
            return ctx.begin();
        }

        template <typename FormatContext>
        auto format(const DB::StorageID & storage_id, FormatContext & ctx) const
        {
            if (storage_id)
                return fmt::format_to(ctx.out(), "{}", storage_id.getFullTableName());
            return fmt::format_to(ctx.out(), "{}", "<empty>");
        }
    };
}


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsBool deduplicate_blocks_in_dependent_materialized_views;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 min_insert_block_size_rows_for_materialized_views;
    extern const SettingsUInt64 min_insert_block_size_bytes_for_materialized_views;
    extern const SettingsBool ignore_materialized_views_with_dropped_target_table;
    extern const SettingsBool distributed_foreground_insert;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsBool insert_null_as_default;
    extern const SettingsBool use_concurrency_control;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool add_implicit_sign_column_constraint_for_collapsing_engine;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}


/// Insert into LiveView.
class PushingToLiveViewSink final : public SinkToStorage
{
public:
    PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, StoragePtr storage_holder_, ContextPtr context_)
        : SinkToStorage(header)
        , live_view(live_view_)
        , storage_holder(std::move(storage_holder_))
        , context(std::move(context_))
    {
    }

    String getName() const override { return "PushingToLiveViewSink"; }
    void consume(Chunk & chunk) override
    {
        Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
        live_view.writeBlock(live_view, getHeader().cloneWithColumns(chunk.getColumns()), std::move(chunk.getChunkInfos()), context);

        if (auto process = context->getProcessListElement())
            process->updateProgressIn(local_progress);

        ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
        ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
    }

private:
    StorageLiveView & live_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

/// Insert into WindowView.
class PushingToWindowViewSink final : public SinkToStorage
{
public:
    PushingToWindowViewSink(const Block & header, StorageWindowView & window_view_, StoragePtr storage_holder_, ContextPtr context_)
        : SinkToStorage(header)
        , window_view(window_view_)
        , storage_holder(std::move(storage_holder_))
        , context(std::move(context_))
    {
    }
    String getName() const override { return "PushingToWindowViewSink"; }
    void consume(Chunk & chunk) override
    {
        Progress local_progress(chunk.getNumRows(), chunk.bytes(), 0);
        StorageWindowView::writeIntoWindowView(
            window_view, getHeader().cloneWithColumns(chunk.getColumns()), std::move(chunk.getChunkInfos()), context);

        if (auto process = context->getProcessListElement())
            process->updateProgressIn(local_progress);

        ProfileEvents::increment(ProfileEvents::SelectedRows, local_progress.read_rows);
        ProfileEvents::increment(ProfileEvents::SelectedBytes, local_progress.read_bytes);
    }

private:
    StorageWindowView & window_view;
    StoragePtr storage_holder;
    ContextPtr context;
};

class FinalizingViewsTransform final : public IProcessor
{
    static InputPorts initPorts(std::vector<Block> headers)
    {
        InputPorts res;
        for (auto & header : headers)
            res.emplace_back(std::move(header));
        return res;
    }

public:
    explicit FinalizingViewsTransform(std::vector<Block> headers)
        : IProcessor(initPorts(std::move(headers)), {Block()})
        , output(outputs.front())
    {
        statuses.resize(getInputs().size());
    }
    String getName() const override { return "FinalizingViewsTransform"; }
    Status prepare() override
    {
        if (output.isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finalize views because output port is finished");
        if (!output.canPush())
            return Status::PortFull;

        if (statuses.empty())
        {
            if (first_exception)
                output.pushException(first_exception);

            output.finish();
            return Status::Finished;
        }

        size_t num_finished = 0;
        size_t i = 0;
        for (auto & input : inputs)
        {
            auto pos = i;
            ++i;

            if (input.isFinished())
            {
                ++num_finished;
                statuses[pos].is_finished = true;
                continue;
            }

            input.setNeeded();
            if (!input.hasData())
                continue;

            auto data = input.pullData();

            if (!data.exception)
                continue;

            if (statuses[pos].exception)
            {
                ++num_finished;
                continue;
            }

            if (!first_exception)
                first_exception = data.exception;

            if (!statuses[pos].exception)
                statuses[pos].exception = data.exception;

            return Status::Ready;
        }

        if (num_finished == inputs.size())
            return Status::Ready;

        return Status::NeedData;
    }

    void work() override
    {
        statuses.clear();
    }

private:
    struct ViewStatus
    {
        bool is_finished = false;
        std::exception_ptr exception;
    };

    OutputPort & output;
    std::vector<ViewStatus> statuses;
    std::exception_ptr first_exception;
};

/// For source chunk, execute view query over it.
template <typename Executor>
class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(
        const Block & input_header, const Block & output_header,
        ASTPtr select_query_,
        StorageID source_id_, StoragePtr source_storage_, StorageMetadataPtr source_storage_metadata_,
        StorageID view_id_, StoragePtr view_storage_, StorageMetadataPtr view_storage_metadata_,
        ContextPtr context_)
        : ExceptionKeepingTransform(input_header, output_header)
        , select_query(select_query_)
        , source_id(source_id_)
        , source_storage(source_storage_)
        , source_storage_metadata(source_storage_metadata_)
        , view_id(view_id_)
        , view_storage(view_storage_)
        , view_storage_metadata(view_storage_metadata_)
        , context(context_)
    {
    }

    String getName() const override { return "ExecutingInnerQueryFromView"; }

protected:
    void onConsume(Chunk chunk) override
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        state.emplace(process(std::move(block), std::move(chunk.getChunkInfos())));
    }

    GenerateResult onGenerate() override
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

private:
    ASTPtr select_query;
    StorageID source_id;
    StoragePtr source_storage;
    StorageMetadataPtr source_storage_metadata;
    StorageID view_id;
    StoragePtr view_storage;
    StorageMetadataPtr view_storage_metadata;
    ContextPtr context;

    struct State
    {
        QueryPipeline pipeline;
        Executor executor;

        explicit State(QueryPipeline pipeline_)
            : pipeline(std::move(pipeline_))
            , executor(pipeline)
        {
        }
    };

    QueryPipeline process(Block data_block, Chunk::ChunkInfoCollection && chunk_infos)
    {
        /// We create a table with the same name as original table and the same alias columns,
        ///  but it will contain single block (that is INSERT-ed into main table).
        /// InterpreterSelectQuery will do processing of alias columns.
        auto local_context = Context::createCopy(context);

        local_context->addViewSource(std::make_shared<StorageValues>(
            source_id,
            source_storage_metadata->getColumns(),
            std::move(data_block),
            *source_storage->getVirtualsPtr()));

        QueryPipelineBuilder pipeline;

        if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter(select_query, local_context, local_context->getViewSource(), SelectQueryOptions().ignoreAccessCheck());
            pipeline = interpreter.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectQuery interpreter(select_query, local_context, SelectQueryOptions().ignoreAccessCheck());
            pipeline = interpreter.buildQueryPipeline();
        }

        pipeline.resize(1);
        pipeline.dropTotalsAndExtremes();

        /// Squashing is needed here because the materialized view query can generate a lot of blocks
        /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
        /// and two-level aggregation is triggered).
        pipeline.addTransform(std::make_shared<SquashingTransform>(
            pipeline.getHeader(),
            context->getSettingsRef()[Setting::min_insert_block_size_rows],
            context->getSettingsRef()[Setting::min_insert_block_size_bytes]));

        pipeline.addTransform(std::make_shared<RestoreChunkInfosTransform>(std::move(chunk_infos), pipeline.getHeader()));

        if (context->getSettingsRef()[Setting::deduplicate_blocks_in_dependent_materialized_views])
        {
            String materialize_view_id = view_id.hasUUID() ? toString(view_id.uuid) : view_id.getFullNameNotQuoted();
            pipeline.addTransform(std::make_shared<DeduplicationToken::SetViewIDTransform>(std::move(materialize_view_id), pipeline.getHeader()));
            pipeline.addTransform(std::make_shared<DeduplicationToken::SetViewBlockNumberTransform>(pipeline.getHeader()));
        }
        else
        {
            pipeline.addTransform(std::make_shared<DeduplicationToken::ResetTokenTransform>(pipeline.getHeader()));
        }

        return QueryPipelineBuilder::getPipeline(std::move(pipeline));
    }

    std::optional<State> state;
};


ViewsManager::ViewsManager(StoragePtr table, ASTPtr query, Block insert_header, ContextPtr context)
    : init_table_id(table->getStorageID())
    , init_storage(table)
    , init_query(query)
    , init_header(std::move(insert_header))
    , init_context(context)
    , logger(getLogger("ViewsManager"))
{
    deduplicate_blocks_in_dependent_materialized_views = init_context->getSettingsRef()[Setting::deduplicate_blocks_in_dependent_materialized_views];

    const ASTInsertQuery * as_insert_query = init_query->as<ASTInsertQuery>();
    insert_null_as_default = as_insert_query && as_insert_query->select && init_context->getSettingsRef()[Setting::insert_null_as_default];

    buildRelaitions();
}


Chain ViewsManager::createPreSink()
{
    return createPreSink({});
}

Chain ViewsManager::createSink()
{
    return createSink({});

}
Chain ViewsManager::createPostSink()
{
    return createPostSink({}, 0);
}

void ViewsManager::buildRelaitions()
{
    LOG_DEBUG(logger, "buildRelaitions: {}", init_table_id);

    auto table_metadata = init_storage->getInMemoryMetadataPtr();

    storages[init_table_id] = init_storage;
    metadata_snapshots[init_table_id] = table_metadata;
    auto lock = init_storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    chassert(lock);
    storage_locks[init_table_id] = std::move(lock);

    inner_tables[{}] = init_table_id;

    select_queries[{}] = init_query->as<ASTInsertQuery>()->select;

    select_contexts[{}] = init_context;
    insert_contexts[{}] = init_context;

    input_headers[{}] = init_header;
    select_headers[{}] = init_header;
    output_headers[{}] = table_metadata->getSampleBlock();

    struct QueueItem
    {
        BundleID parent;
        BundleID current;
    };

    std::queue<QueueItem> bfs_q;
    bfs_q.push({/*parent*/{}, /*current*/{{}, init_table_id}});

    while (!bfs_q.empty())
    {
        auto [parent, current] = bfs_q.front();
        bfs_q.pop();

        LOG_DEBUG(logger, "relation: {} ({}) --> {} ({})", parent.view_id, parent.inner_id, current.view_id, current.inner_id);

        auto children = DatabaseCatalog::instance().getDependentViews(current.inner_id);
        dependent_views[current.view_id] = {};
        dependent_views[current.view_id].reserve(children.size());

        for (const auto & child_id : children)
        {
            auto storage = DatabaseCatalog::instance().tryGetTable(child_id, init_context);
            if (!storage)
            {
                LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", child_id.getFullTableName());
                continue;
            }

            auto child_lock = storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);
            if (child_lock == nullptr)
            {
                // In case the materialized view is dropped/detached at this point, we register a warning and ignore it
                assert(storage->is_dropped || storage->is_detached);
                LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", child_id.getFullTableName());
                continue;
            }

            auto child_metadata = storage->getInMemoryMetadataPtr();

            auto parent_select_context = select_contexts.at(parent.view_id);
            auto select_context = child_metadata->getSQLSecurityOverriddenContext(parent_select_context);
            select_context->setQueryAccessInfo(parent_select_context->getQueryAccessInfoPtr());
            // Processing of blocks for MVs is done block by block, and there will
            // be no parallel reading after (plus it is not a costless operation)
            select_context->setSetting("parallelize_output_from_storages", Field{false});

            auto insert_context = Context::createCopy(select_context);
            if (!deduplicate_blocks_in_dependent_materialized_views)
                insert_context->setSetting("insert_deduplicate", Field{false});

            const auto & insert_settings = insert_context->getSettingsRef();
            // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
            if (insert_settings[Setting::min_insert_block_size_rows_for_materialized_views])
                insert_context->setSetting("min_insert_block_size_rows", insert_settings[Setting::min_insert_block_size_rows_for_materialized_views].value);
            if (insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views])
                insert_context->setSetting("min_insert_block_size_bytes", insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views].value);

            if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
            {
                auto inner_table_id = materialized_view->getTargetTableId();
                auto inner_table_storage = DatabaseCatalog::instance().tryGetTable(inner_table_id, init_context);

                /// If target table was dropped, ignore this materialized view.
                if (!inner_table_storage)
                {
                    if (init_context->getSettingsRef()[Setting::ignore_materialized_views_with_dropped_target_table])
                        continue;

                    throw Exception(
                        ErrorCodes::UNKNOWN_TABLE,
                        "Target table '{}' of view '{}' doesn't exists. To ignore this view use setting "
                        "ignore_materialized_views_with_dropped_target_table",
                        inner_table_id.getFullTableName(),
                        child_id.getFullTableName());
                }

                auto inner_metadata_snapshot = inner_table_storage->getInMemoryMetadataPtr();

                auto inner_lock = inner_table_storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (inner_lock == nullptr)
                {
                    // In case the materialized view is dropped/detached at this point, we register a warning and ignore it
                    assert(inner_table_storage->is_dropped || inner_table_storage->is_detached);
                    LOG_WARNING(logger, "Trying to access table {} but it doesn't exist", child_id);
                    continue;
                }

                const auto & select_table_id = child_metadata->getSelectQuery().select_table_id;
                if (select_table_id != current.inner_id)
                {
                    /// It may happen if materialize view query was changed and it doesn't depend on this source table anymore.
                    /// See setting `allow_experimental_alter_materialized_view_structure`
                    LOG_DEBUG(logger, "Table '{}' is not a source for view '{}' anymore, current source is '{}'",
                        current.inner_id, child_id, select_table_id);
                    continue;
                }
                auto select_query = child_metadata->getSelectQuery().inner_query;


                Block select_header;
                // Get list of columns we get from select query.
                if (select_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                    select_header = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query, select_context);
                else
                    select_header = InterpreterSelectQuery(select_query, select_context, SelectQueryOptions()).getSampleBlock();

                storages[inner_table_id] = inner_table_storage;
                storage_locks[inner_table_id] = std::move(inner_lock);
                metadata_snapshots[inner_table_id] = inner_metadata_snapshot;

                inner_tables[child_id] = inner_table_id;

                select_queries[child_id] = select_query;
                input_headers[child_id] = output_headers.at(current.view_id);
                select_headers[child_id] = select_header;
                output_headers[child_id] = inner_metadata_snapshot->getSampleBlock();

                bfs_q.push({current, {child_id, inner_table_id}});
            }
            else if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
            {
                select_queries[child_id] = live_view->getInnerQuery();
                inner_tables[child_id] = child_id;

                bfs_q.push({current, {child_id, child_id}});
            }
            else if (auto * window_view = dynamic_cast<StorageWindowView *>(storage.get()))
            {
                select_queries[child_id] = window_view->getMergeableQuery();
                inner_tables[child_id] = child_id;

                bfs_q.push({current, {child_id, child_id}});
            }

            storages[child_id] = storage;
            metadata_snapshots[child_id] = child_metadata;
            storage_locks[child_id] = std::move(child_lock);

            select_contexts[child_id] = select_context;
            insert_contexts[child_id] = insert_context;
            dependent_views[current.view_id].push_back(child_id);
            source_tables[child_id] = current.inner_id;
        }
    }
}


Chain ViewsManager::createRetry(Dependencies path)
{
    LOG_DEBUG(logger, "createRetry: {}", path.getLast().inner_id);

    if (path.getLast().inner_id == init_table_id)
    {
        return createPreSink(init_table_id);
    }

    std::stack<Chain> partial_results;
    while (!path.empty())
    {
        auto [view_id, inner_id] = path.getLast();
        Chain chain;
        chain.appendChainNotStrict(createSelect(view_id));
        chain.appendChainNotStrict(createPreSink(inner_id));
        partial_results.push(std::move(chain));
    }

    Chain result;
    while (!partial_results.empty())
    {
        result.appendChainNotStrict(std::move(partial_results.top()));
        partial_results.pop();
    }

    return result;
}


Chain ViewsManager::createSelect(StorageIDPrivate view_id)
{
    LOG_DEBUG(logger, "select generator: {}", view_id);

    if (view_id == init_table_id)
        return {};

    Chain result;

    auto storage = storages.at(view_id);
    auto select_query = select_queries.at(view_id);
    auto select_context = select_contexts.at(view_id);
    auto insert_context = select_contexts.at(view_id);


    if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        auto inner_table_id = materialized_view->getTargetTableId();
        auto inner_table_storage = storages.at(inner_table_id);
        auto inner_metadata_snapshot = metadata_snapshots.at(inner_table_id);

        Block select_header = select_headers.at(view_id);

        bool async_insert = false;
        bool no_squash = false;
        bool should_add_squashing = !(insert_context->getSettingsRef()[Setting::distributed_foreground_insert] && inner_table_storage->isRemote()) && !async_insert && !no_squash;
        if (should_add_squashing)
        {
            bool table_prefers_large_blocks = inner_table_storage->prefersLargeBlocks();
            const auto & settings = insert_context->getSettingsRef();

            result.addSource(std::make_shared<SquashingTransform>(
                select_header,
                table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL));
        }

#ifdef DEBUG_OR_SANITIZER_BUILD
        result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Before squashing", select_header));
#endif

        auto counting = std::make_shared<CountingTransform>(select_header, current_thread, insert_context->getQuota());
        counting->setProcessListElement(insert_context->getProcessListElement());
        counting->setProgressCallback(insert_context->getProgressCallback());
        result.addSource(std::move(counting));

        result.addStorageHolder(storage);
        result.addStorageHolder(inner_table_storage);

#ifdef DEBUG_OR_SANITIZER_BUILD
        result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right after Inner query", select_header));
#endif

        auto source_table_id = source_tables.at(view_id);

        auto input_header = input_headers.at(view_id);

        LOG_DEBUG(logger, "select generator: source {},input_header {} ", source_table_id, input_header.dumpStructure());


        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingPipelineExecutor>>(
            input_header, select_header,
            select_query,
            source_table_id, storages.at(source_table_id), metadata_snapshots.at(source_table_id),
            view_id, storage, metadata_snapshots.at(view_id),
            insert_context);
        //executing_inner_query->setRuntimeData(view_thread_status, view_counter_ms);
        result.addSource(std::move(executing_inner_query));

#ifdef DEBUG_OR_SANITIZER_BUILD
        result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right before Inner query", input_header));
#endif
    }

    LOG_DEBUG(logger, "select generator: {}, input {}, output {}", view_id.getNameForLogs(), result.getInputHeader().dumpStructure(), result.getOutputHeader().dumpStructure());

    return result;
}


Chain ViewsManager::createPreSink(StorageIDPrivate view_id)
{
    LOG_DEBUG(logger, "createPreSink: {}", view_id.getNameForLogs());

    /// We create a pipeline of several streams, into which we will write data.
    Chain chain;

    auto select_context = select_contexts.at(view_id);
    auto insert_context = insert_contexts.at(view_id);

    auto select_header = select_headers.at(view_id);

    auto inner_id = inner_tables.at(view_id);
    LOG_DEBUG(logger, "createPreSink: {}, inner id {}", view_id.getNameForLogs(), inner_id.getNameForLogs());

    auto inner_storage = storages.at(inner_id);
    auto inner_metadata_snapshot = metadata_snapshots.at(inner_id);
    auto inner_storage_header = inner_metadata_snapshot->getSampleBlock();

    auto adding_missing_defaults_dag = addMissingDefaults(
        select_header,
        inner_storage_header.getNamesAndTypesList(),
        inner_metadata_snapshot->getColumns(),
        insert_context,
        insert_null_as_default);

    auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(select_header, adding_missing_defaults_dag.getRequiredColumnsNames(), insert_context);
    auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(adding_missing_defaults_dag)));

    LOG_DEBUG(logger, "createPreSink: {}, transformed header add default {}", view_id.getNameForLogs(), ExpressionTransform::transformHeader(select_header, adding_missing_defaults_actions->getActionsDAG()).dumpStructure());

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    chain.addSink(std::make_shared<ConvertingTransform>(select_header, adding_missing_defaults_actions));

    auto converting = ActionsDAG::makeConvertingActions(
        chain.getOutputHeader().getColumnsWithTypeAndName(),
        inner_storage_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto convert_action = std::make_shared<ExpressionActions>(std::move(converting));

    LOG_DEBUG(logger, "createPreSink: {}, transformed header cast types {}", view_id.getNameForLogs(), ExpressionTransform::transformHeader(chain.getOutputHeader(), convert_action->getActionsDAG()).dumpStructure());

    chain.addSink(std::make_shared<ExpressionTransform>(chain.getOutputHeader(), convert_action));

    /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.
    /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
    /// Add implicit sign constraint for Collapsing and VersionedCollapsing tables.
    auto constraints = inner_metadata_snapshot->getConstraints();
    auto storage_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(inner_storage);
    if (storage_merge_tree
        && (storage_merge_tree->merging_params.mode == MergeTreeData::MergingParams::Collapsing
            || storage_merge_tree->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        && (*storage_merge_tree->getSettings())[MergeTreeSetting::add_implicit_sign_column_constraint_for_collapsing_engine])
    {
        auto sign_column_check_constraint = std::make_unique<ASTConstraintDeclaration>();
        sign_column_check_constraint->name = "_implicit_sign_column_constraint";
        sign_column_check_constraint->type = ASTConstraintDeclaration::Type::CHECK;

        Array valid_values_array;
        valid_values_array.emplace_back(-1);
        valid_values_array.emplace_back(1);

        auto valid_values_ast = std::make_unique<ASTLiteral>(std::move(valid_values_array));
        auto sign_column_ast = std::make_unique<ASTIdentifier>(storage_merge_tree->merging_params.sign_column);
        sign_column_check_constraint->set(sign_column_check_constraint->expr, makeASTFunction("in", std::move(sign_column_ast), std::move(valid_values_ast)));

        auto constraints_ast = constraints.getConstraints();
        constraints_ast.push_back(std::move(sign_column_check_constraint));
        constraints = ConstraintsDescription(constraints_ast);
    }
    if (!constraints.empty())
        chain.addSink(std::make_shared<CheckConstraintsTransform>(inner_id, inner_storage_header, constraints, insert_context));

    /// Add transform to check if the sizes of arrays - elements of nested data structures doesn't match.
    /// We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
    /// NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
    /// but currently we don't have methods for serialization of nested structures "as a whole".
    chain.addSink(std::make_shared<NestedElementsValidationTransform>(inner_storage_header));

    LOG_DEBUG(logger, "createPreSink: {}, input {}, output {}", view_id.getNameForLogs(), chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createSink(StorageIDPrivate view_id)
{
    auto inner_id = inner_tables.at(view_id);
    LOG_DEBUG(logger, "createSink: {} ({})", view_id, inner_id);

    Chain chain;

    if (view_id)
    {
        auto view_storage = storages.at(view_id);

        IInterpreter::checkStorageSupportsTransactionsIfNeeded(view_storage, insert_contexts.at(view_id));

        if (auto * live_view = dynamic_cast<StorageLiveView *>(view_storage.get()))
        {
            auto sink = std::make_shared<PushingToLiveViewSink>(select_headers.at(view_id), *live_view, view_storage, insert_contexts.at(view_id));
            // sink->setRuntimeData(thread_status, elapsed_counter_ms);
            sink->setViewManager(shared_from_this());
            chain.addSource(std::move(sink));
        }
        else if (auto * window_view = dynamic_cast<StorageWindowView *>(view_storage.get()))
        {
            auto sink = std::make_shared<PushingToWindowViewSink>(window_view->getInputHeader(), *window_view, view_storage, insert_contexts.at(view_id));
            //sink->setRuntimeData(thread_status, elapsed_counter_ms);
            sink->setViewManager(shared_from_this());
            chain.addSource(std::move(sink));
        }
        else if (dynamic_cast<StorageMaterializedView *>(view_storage.get()))
        {
            LOG_DEBUG(logger, "createSink: {}, for StorageMaterializedView", view_id.getNameForLogs());

            auto sink = view_storage->write(select_queries.at(view_id), metadata_snapshots.at(view_id), insert_contexts.at(view_id), /*async_insert*/ false);

            LOG_DEBUG(logger, "createSink: {}, sink structure: {}", view_id, sink->getHeader().dumpStructure());

            //metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
            //sink->setRuntimeData(thread_status, elapsed_counter_ms);
            sink->setViewManager(shared_from_this());
            chain.addSource(std::move(sink));
        }
        else
        {
            UNREACHABLE();
        }
    }
    else
    {
        LOG_DEBUG(logger, "createSink: {}, for not a view", inner_id);
        chassert(inner_id == init_table_id);

        IInterpreter::checkStorageSupportsTransactionsIfNeeded(storages.at(inner_id), insert_contexts.at(view_id));

        auto sink = storages.at(inner_id)->write(select_queries.at(view_id), metadata_snapshots.at(inner_id), insert_contexts.at(view_id), false);
        metadata_snapshots.at(inner_id)->check(sink->getHeader().getColumnsWithTypeAndName());
        //sink->setRuntimeData(thread_status, elapsed_counter_ms);
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }

    chain.addSink(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(chain.getOutputHeader()));

    LOG_DEBUG(logger, "createSink: {} ({}) input {}, output {}", view_id, inner_id, chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createPostSink(StorageIDPrivate view_id, size_t level)
{
    auto inner_table = inner_tables.at(view_id);
    LOG_DEBUG(logger, "createPostSink: {} ({})", view_id, inner_table);


    auto & children = dependent_views.at(view_id);
    if (children.empty())
        return {};

    std::vector<Chain> view_chains;
    view_chains.reserve(children.size());

    std::vector<Block> output_view_chains_headers;
    output_view_chains_headers.reserve(children.size());

    for (auto & child_view_id : children)
    {
        LOG_DEBUG(logger, "createPostSink: {} --> {}", view_id, child_view_id);

        ProfileEvents::increment(ProfileEvents::InsertQueriesWithSubqueries);
        ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);

        Chain chain;
        {
            auto tmp = createSelect(child_view_id);
            LOG_DEBUG(logger, "createPostSink: {} ({}) --> {},"
                " createSelect with input header {} || output header {}", view_id, inner_table, child_view_id, tmp.getInputHeader().dumpStructure(), tmp.getOutputHeader().dumpStructure());
            chain.appendChainNotStrict(std::move(tmp));
        }
        {
            auto tmp = createPreSink(child_view_id);
            LOG_DEBUG(logger, "createPostSink: {} ({}) --> {},"
                " createPreSink with input header {} || output header {}", view_id, inner_table, child_view_id, tmp.getInputHeader().dumpStructure(), tmp.getOutputHeader().dumpStructure());
            chain.appendChainNotStrict(std::move(tmp));
        }
        {
            auto tmp = createSink(child_view_id);
            LOG_DEBUG(logger, "createPostSink: {} ({}) --> {},"
                " createSink with input header {} || output header {}", view_id, inner_table, child_view_id, tmp.getInputHeader().dumpStructure(), tmp.getOutputHeader().dumpStructure());
            chain.appendChainNotStrict(std::move(tmp));
        }
        {
            auto tmp = createPostSink(child_view_id, level+1);
            if (!tmp.empty())
                LOG_DEBUG(logger, "createPostSink: {} ({}) --> {},"
                    " createPostSink with input header {} || output header {}", view_id, inner_table, child_view_id, tmp.getInputHeader().dumpStructure(), tmp.getOutputHeader().dumpStructure());
            chain.appendChainNotStrict(std::move(tmp));
        }

        LOG_DEBUG(logger, "createPostSink: {} --> {}, input header {}, output header {}", view_id, child_view_id, chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

        output_view_chains_headers.push_back(chain.getOutputHeader());
        view_chains.push_back(std::move(chain));
    }


    auto copying_data = std::make_shared<CopyTransform>(output_headers.at(view_id), children.size());
    auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(output_view_chains_headers));
    auto out = copying_data->getOutputs().begin();
    auto in = finalizing_views->getInputs().begin();

    LOG_DEBUG(logger, "createPostSink: {} ({}) append {} VS {}", view_id, inner_table, out->getHeader().dumpStructure(), in->getHeader().dumpStructure());

    std::list<ProcessorPtr> processors;
    QueryPlanResourceHolder resources;
    size_t max_parallel_streams = 0;
    for (auto & chain : view_chains)
    {
        max_parallel_streams += std::max<size_t>(chain.getNumThreads(), 1);
        resources.append(chain.detachResources());
        connect(*out, chain.getInputPort());
        connect(chain.getOutputPort(), *in);
        ++in;
        ++out;
        processors.splice(processors.end(), Chain::getProcessors(std::move(chain)));
    }

    processors.emplace_front(std::move(copying_data));
    processors.emplace_back(std::move(finalizing_views));

    auto result = Chain(std::move(processors));
    result.attachResources(std::move(resources));
    result.setNumThreads(max_parallel_streams);
    result.setConcurrencyControl(insert_contexts.at(view_id)->getSettingsRef()[Setting::use_concurrency_control]);

    LOG_DEBUG(logger, "createPostSink: {}, input {}, output {}", view_id, result.getInputHeader().dumpStructure(), result.getOutputHeader().dumpStructure());

    return result;
}

}
