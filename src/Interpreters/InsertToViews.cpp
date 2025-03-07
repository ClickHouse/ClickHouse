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

#include <IO/Progress.h>

#include <Core/Settings.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
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


void ViewsManager::buildRelaitions()
{
    std::queue<StorageID> bfs_q;
    bfs_q.push(init_id);

    auto table_storage = DatabaseCatalog::instance().tryGetTable(init_id, init_context);
    chassert(table_storage);
    auto table_metadata = table_storage->getInMemoryMetadataPtr();

    parents[init_id] = init_id;
    inner_storages[init_id] = init_id;
    storages[init_id] = table_storage;
    metadata_snapshots[init_id] = table_metadata;
    select_queries[init_id] = init_query->as<ASTInsertQuery>()->select;
    auto lock = table_storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    chassert(lock);
    storage_locks[init_id] = std::move(lock);
    select_contexts[init_id] = init_context;
    insert_contexts[init_id] = init_context;

    insert_headers[init_id] = table_metadata->getSampleBlock();
    select_headers[init_id] = init_header;

    while (!bfs_q.empty())
    {
        auto parent_id = bfs_q.front();
        bfs_q.pop();

        auto children_ids = DatabaseCatalog::instance().getDependentViews(parent_id);
        children[parent_id] = {};
        children[parent_id].reserve(children_ids.size());

        for (const auto & child_id : children_ids)
        {
            LOG_DEBUG(getLogger("ViewsManager"), "relation: {} --> {}", parent_id.getNameForLogs(), child_id.getNameForLogs());

            if (parents.contains(child_id))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "there is a cycle {}", child_id.getFullTableName());

            auto child_storage = DatabaseCatalog::instance().tryGetTable(child_id, init_context);
            if (!child_storage)
            {
                LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", child_id.getFullTableName());
                continue;
            }

            auto child_lock = child_storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);

            if (child_lock == nullptr)
            {
                // In case the materialized view is dropped/detached at this point, we register a warning and ignore it
                assert(child_storage->is_dropped || child_storage->is_detached);
                LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", child_id.getFullTableName());
                continue;
            }

            auto child_metadata = child_storage->getInMemoryMetadataPtr();

            auto parent_select_context = select_contexts.at(parent_id);
            auto select_context = child_metadata->getSQLSecurityOverriddenContext(parent_select_context);
            select_context->setQueryAccessInfo(parent_select_context->getQueryAccessInfoPtr());
            // Processing of blocks for MVs is done block by block, and there will
            // be no parallel reading after (plus it is not a costless operation)
            select_context->setSetting("parallelize_output_from_storages", Field{false});

            auto insert_context = Context::createCopy(select_context);
            if (deduplicate_blocks_in_dependent_materialized_views)
                insert_context->setSetting("insert_deduplicate", Field{false});

            const auto & insert_settings = insert_context->getSettingsRef();
            // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
            if (insert_settings[Setting::min_insert_block_size_rows_for_materialized_views])
                insert_context->setSetting("min_insert_block_size_rows", insert_settings[Setting::min_insert_block_size_rows_for_materialized_views].value);
            if (insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views])
                insert_context->setSetting("min_insert_block_size_bytes", insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views].value);

            if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(child_storage.get()))
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
                    LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", child_id.getFullTableName());
                    continue;
                }

                const auto & select_query = child_metadata->getSelectQuery();
                if (select_query.select_table_id != parent_id)
                {
                    /// It may happen if materialize view query was changed and it doesn't depend on this source table anymore.
                    /// See setting `allow_experimental_alter_materialized_view_structure`
                    LOG_DEBUG(
                        getLogger("ViewsManager"), "Table '{}' is not a source for view '{}' anymore, current source is '{}'",
                        parent_id.getFullTableName(), child_id.getFullTableName(), select_query.select_table_id.getFullTableName());
                    continue;
                }


                // Block select_header;
                /// Get list of columns we get from select query.
                // if (select_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                //     select_header = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query, select_context);
                // else
                //     select_header = InterpreterSelectQuery(select_query, select_context, SelectQueryOptions()).getSampleBlock();

                Block select_header = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query.inner_query, select_context);

                select_queries[child_id] = select_query.inner_query;
                select_headers[child_id] = select_header;
                insert_headers[child_id] = metadata_snapshots.at(parent_id)->getSampleBlock();

                inner_storages[child_id] = inner_table_id;
                storages[inner_table_id] = inner_table_storage;
                storage_locks[inner_table_id] = std::move(inner_lock);
                metadata_snapshots[inner_table_id] = inner_metadata_snapshot;
            }
            else if (auto * live_view = dynamic_cast<StorageLiveView *>(child_storage.get()))
            {
                select_queries[child_id] = live_view->getInnerQuery();
                inner_storages[child_id] = child_id;
            }
            else if (auto * window_view = dynamic_cast<StorageWindowView *>(child_storage.get()))
            {
                select_queries[child_id] = window_view->getMergeableQuery();
                inner_storages[child_id] = child_id;
            }

            bfs_q.push(child_id);

            parents[child_id] = parent_id;
            storages[child_id] = child_storage;
            metadata_snapshots[child_id] = child_metadata;
            storage_locks[child_id] = std::move(child_lock);
            select_contexts[child_id] = select_context;
            insert_contexts[child_id] = insert_context;
            children[parent_id].push_back(child_id);
        }
    }
}


Chain ViewsManager::createRetry(StorageID t_id)
{
    LOG_DEBUG(getLogger("ViewsManager"), "createRetry: {}", t_id.getNameForLogs());

    if (t_id == init_id)
    {
        return createPreSink(init_id);
    }

    Chain result;
    result.appendChainNotStrict(createRetry(parents.at(t_id)))
        .appendChainNotStrict(createSelect(t_id))
        .appendChainNotStrict(createPreSink(t_id));
    return result;
}


Chain ViewsManager::createSelect(StorageID t_id)
{
    LOG_DEBUG(getLogger("ViewsManager"), "select generator: {}", t_id.getNameForLogs());

    if (t_id == init_id)
        return {};

    Chain result;

    auto storage = storages.at(t_id);
    auto select_query = select_queries.at(t_id);
    auto select_context = select_contexts.at(t_id);
    auto insert_context = select_contexts.at(t_id);


    if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        auto inner_table_id = materialized_view->getTargetTableId();
        auto inner_table_storage = storages.at(inner_table_id);
        auto inner_metadata_snapshot = metadata_snapshots.at(inner_table_id);

        Block select_header = select_headers.at(t_id);

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

        auto parent_id = parents.at(t_id);
        auto input_header = insert_headers.at(t_id);

        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingPipelineExecutor>>(
            input_header, select_header,
            select_query,
            parent_id, storages.at(parent_id), metadata_snapshots.at(parent_id),
            t_id, storage, metadata_snapshots.at(t_id),
            insert_context);
        //executing_inner_query->setRuntimeData(view_thread_status, view_counter_ms);
        result.addSource(std::move(executing_inner_query));

#ifdef DEBUG_OR_SANITIZER_BUILD
        result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right before Inner query", input_header));
#endif
    }

    LOG_DEBUG(getLogger("ViewsManager"), "select generator: {}, input {}, output {}", t_id.getNameForLogs(), result.getInputHeader().dumpStructure(), result.getOutputHeader().dumpStructure());

    return result;
}


ViewsManager::ViewsManager(StorageID table_id, ASTPtr query, Block insert_header, ContextPtr context)
    : init_id(table_id)
    , init_query(query)
    , init_header(std::move(insert_header))
    , init_context(context)
{
    buildRelaitions();

    deduplicate_blocks_in_dependent_materialized_views = init_context->getSettingsRef()[Setting::deduplicate_blocks_in_dependent_materialized_views];

    const ASTInsertQuery * as_insert_query = init_query->as<ASTInsertQuery>();
    insert_null_as_default = as_insert_query && as_insert_query->select && init_context->getSettingsRef()[Setting::insert_null_as_default];
}


Chain ViewsManager::createPreSink(StorageID t_id)
{
    LOG_DEBUG(getLogger("ViewsManager"), "createPreSink: {}", t_id.getNameForLogs());

    /// We create a pipeline of several streams, into which we will write data.
    Chain chain;

    auto select_context = select_contexts.at(t_id);
    auto insert_context = insert_contexts.at(t_id);

    auto select_header = select_headers.at(t_id);

    auto inner_id = inner_storages.at(t_id);
    LOG_DEBUG(getLogger("ViewsManager"), "createPreSink: {}, inner id {}", t_id.getNameForLogs(), inner_id.getNameForLogs());

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

    LOG_DEBUG(getLogger("ViewsManager"), "createPreSink: {}, transformed header add default {}", t_id.getNameForLogs(), ExpressionTransform::transformHeader(select_header, adding_missing_defaults_actions->getActionsDAG()).dumpStructure());

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    chain.addSink(std::make_shared<ConvertingTransform>(select_header, adding_missing_defaults_actions));

    auto converting = ActionsDAG::makeConvertingActions(
        chain.getOutputHeader().getColumnsWithTypeAndName(),
        inner_storage_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto convert_action = std::make_shared<ExpressionActions>(std::move(converting));

    LOG_DEBUG(getLogger("ViewsManager"), "createPreSink: {}, transformed header cast types {}", t_id.getNameForLogs(), ExpressionTransform::transformHeader(chain.getOutputHeader(), convert_action->getActionsDAG()).dumpStructure());

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

    LOG_DEBUG(getLogger("ViewsManager"), "createPreSink: {}, input {}, output {}", t_id.getNameForLogs(), chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createSink(StorageID t_id)
{
    LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}", t_id.getNameForLogs());

    Chain chain;

    auto metadata_snapshot = metadata_snapshots.at(t_id);
    auto storage = storages.at(t_id);
    auto select_query = select_queries.at(t_id);
    auto select_header = select_headers.at(t_id);
    auto insert_context = insert_contexts.at(t_id);

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        auto sink = std::make_shared<PushingToLiveViewSink>(select_header, *live_view, storage, insert_context);
        // sink->setRuntimeData(thread_status, elapsed_counter_ms);
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }
    else if (auto * window_view = dynamic_cast<StorageWindowView *>(storage.get()))
    {
        auto sink = std::make_shared<PushingToWindowViewSink>(window_view->getInputHeader(), *window_view, storage, insert_context);
        //sink->setRuntimeData(thread_status, elapsed_counter_ms);
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }
    else if (dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}, for StorageMaterializedView", t_id.getNameForLogs());

        LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}, medatada structure: {}", t_id, metadata_snapshot->getSampleBlock().dumpStructure());

        auto sink = storage->write(select_query, metadata_snapshot, insert_context, /*async_insert*/ false);

        LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}, sink structure: {}", t_id, sink->getHeader().dumpStructure());

        //metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        //sink->setRuntimeData(thread_status, elapsed_counter_ms);
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }
    else
    {
        LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}, for not a view", t_id.getNameForLogs());

        chassert(t_id == init_id);

        auto sink = storage->write(select_query, metadata_snapshot, insert_context, false);
        metadata_snapshot->check(sink->getHeader().getColumnsWithTypeAndName());
        //sink->setRuntimeData(thread_status, elapsed_counter_ms);
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }

    chain.addSink(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(chain.getOutputHeader()));

    LOG_DEBUG(getLogger("ViewsManager"), "createSink: {}, input {}, output {}", t_id.getNameForLogs(), chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createPostSink(StorageID t_id, size_t level)
{
    LOG_DEBUG(getLogger("ViewsManager"), "createPostSink: {}", t_id.getNameForLogs());

    auto & current_children = children.at(t_id);

    if (current_children.empty())
        return {};

    std::vector<Chain> view_chains;
    view_chains.reserve(current_children.size());

    std::vector<Block> output_headers;
    output_headers.reserve(current_children.size());

    for (auto & child_id : current_children)
    {
        LOG_DEBUG(getLogger("ViewsManager"), "createPostSink: {} --> {}", t_id.getNameForLogs(), child_id.getNameForLogs());

        Chain chain;
        chain.appendChainNotStrict(createSelect(child_id));
        chain.appendChainNotStrict(createPreSink(child_id));
        chain.appendChainNotStrict(createSink(child_id));
        chain.appendChainNotStrict(createPostSink(child_id, level+1));

        output_headers.push_back(chain.getOutputHeader());
        view_chains.push_back(std::move(chain));
    }

    auto copying_data = std::make_shared<CopyTransform>(insert_headers.at(t_id), current_children.size());
    auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(output_headers));
    auto out = copying_data->getOutputs().begin();
    auto in = finalizing_views->getInputs().begin();

    std::list<ProcessorPtr> processors;
    QueryPlanResourceHolder resourses;
    size_t max_parallel_streams = 0;
    for (auto & chain : view_chains)
    {
        max_parallel_streams += std::max<size_t>(chain.getNumThreads(), 1);
        resourses.append(chain.detachResources());
        connect(*out, chain.getInputPort());
        connect(chain.getOutputPort(), *in);
        ++in;
        ++out;
        processors.splice(processors.end(), Chain::getProcessors(std::move(chain)));
    }

    processors.emplace_front(std::move(copying_data));
    processors.emplace_back(std::move(finalizing_views));

    auto result = Chain(std::move(processors));
    result.attachResources(std::move(resourses));
    result.setNumThreads(max_parallel_streams);
    result.setConcurrencyControl(insert_contexts.at(t_id)->getSettingsRef()[Setting::use_concurrency_control]);

    LOG_DEBUG(getLogger("ViewsManager"), "createPostSink: {}, input {}, output {}", t_id.getNameForLogs(), result.getInputHeader().dumpStructure(), result.getOutputHeader().dumpStructure());

    return result;
}

struct MakeSharedEnabler : public ViewsManager
{
    template <class... Args>
    explicit MakeSharedEnabler(Args &&... args) : ViewsManager(std::forward<Args>(args)...) { }
};

ViewsManager::Ptr ViewsManager::create(StorageID table_id, ASTPtr query, Block insert_header, ContextPtr context)
{
    return std::make_shared<MakeSharedEnabler>(table_id, query, insert_header, context);
}

}
