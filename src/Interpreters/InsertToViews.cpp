#include <functional>
#include <stack>
#include <vector>
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
#include <Interpreters/QueryViewsLog.h>
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
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/formatAST.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <IO/Progress.h>

#include <Core/Settings.h>
#include "Common/CurrentThread.h"
#include "Common/Logger.h"
#include "Common/ThreadStatus.h"
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/logger_useful.h>
#include "Interpreters/StorageID.h"
#include "Processors/Chunk.h"
#include "QueryPipeline/Chain.h"
#include "base/defines.h"
#include "base/scope_guard.h"

namespace ProfileEvents
{
    extern const Event InsertQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
    extern const Event InsertedRows;
    extern const Event InsertedBytes;
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
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsBool log_query_views;
    extern const SettingsBool log_queries;
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsBool log_profile_events;
    extern const SettingsBool calculate_text_stack_trace;
    extern const SettingsBool use_async_executor_for_materialized_views;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool add_implicit_sign_column_constraint_for_collapsing_engine;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
}


/// Insert into LiveView.
class PushingToLiveViewSink final : public SinkToStorage
{
public:
    PushingToLiveViewSink(const Block & header, StorageLiveView & live_view_, ContextPtr context_)
        : SinkToStorage(header)
        , live_view(live_view_)
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
    ContextPtr context;
};

/// Insert into WindowView.
class PushingToWindowViewSink final : public SinkToStorage
{
public:
    PushingToWindowViewSink(const Block & header, StorageWindowView & window_view_, ContextPtr context_)
        : SinkToStorage(header)
        , window_view(window_view_)
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
    explicit FinalizingViewsTransform(std::vector<Block> headers, std::vector<ViewsManager::StorageIDPrivate> views, ViewsManager::ConstPtr views_manager_)
        : IProcessor(initPorts(std::move(headers)), {Block()})
        , output(outputs.front())
        , views_manager(views_manager_)
    {
        chassert(inputs.size() == views.size());

        statuses.reserve(views.size());
        for (auto & view_id : views)
        {
            statuses.emplace_back(std::move(view_id));
        }
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
        for (auto & status : statuses)
        {
            views_manager->logQueryView(status.view_id, status.exception);
        }

        statuses.clear();
    }

private:
    struct ViewStatus
    {
        explicit ViewStatus(StorageID view_id_)
            : view_id(std::move(view_id_))
        {}
        StorageID view_id;
        bool is_finished = false;
        std::exception_ptr exception;
    };

    OutputPort & output;

    ViewsManager::ConstPtr views_manager;
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

        LOG_DEBUG(getLogger("InnerSelect"), "generate rows {} data {}", res.chunk.getNumRows(), res.chunk.dumpStructure());
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

        LOG_DEBUG(getLogger("InnerSelect"), "source_id {}, data {}", source_id, data_block.rows());

        local_context->addViewSource(std::make_shared<StorageValues>(
            source_id,
            source_storage_metadata->getColumns(),
            std::move(data_block),
            *source_storage->getVirtualsPtr()));

        QueryPipelineBuilder pipeline;

        if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            LOG_DEBUG(getLogger("InnerSelect"), "A");

            //local_context->getViewSource()
            InterpreterSelectQueryAnalyzer interpreter(select_query, local_context,local_context->getViewSource(), SelectQueryOptions().ignoreAccessCheck());
            pipeline = interpreter.buildQueryPipeline();
        }
        else
        {
            LOG_DEBUG(getLogger("InnerSelect"), "B");
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


Chain ViewsManager::createPreSink() const
{
    return createPreSink(root.view_id);
}


Chain ViewsManager::createSink() const
{
    return createSink(root.view_id);

}


Chain ViewsManager::createPostSink() const
{
    return createPostSink(root.view_id, 0);
}


void ViewsManager::buildRelaitions()
{
    LOG_DEBUG(logger, "buildRelaitions: {}", init_table_id);

    class VisitedPath
    {
        std::vector<StorageIDPrivate> path;
        std::set<StorageIDPrivate> visited;

        StorageIDPrivate empty_id = {};

    public:
        void pushBack(StorageIDPrivate id)
        {
            if (visited.contains(id))
                throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
                    "Dependencies of the table {} are cyclic. Cycle is {}", path.front(), fmt::join(path, " :-> "));

            path.push_back(id);
            visited.insert(id);
        }

        void popBack()
        {
            visited.erase(path.back());
            path.pop_back();
        }


        [[maybe_unused]] bool empty() const { return path.empty(); }
        const StorageIDPrivate & back() const { return path.back(); }
        const StorageIDPrivate & current() const { return back(); }
        const StorageIDPrivate & parent() const { if (path.size() > 1) return *++path.rbegin(); return empty_id; }
        const StorageIDPrivate & prevParent() const { if (path.size() > 2) return *++++path.rbegin(); return empty_id; }
        const StorageIDPrivate & prevPrevParent() const { if (path.size() > 3) return *++++++path.rbegin(); return empty_id; }
        String debugString() const { return fmt::format("{}", fmt::join(path, " :-> ")); }
    };

    auto register_path = [&] (const VisitedPath & path)
    {
        auto parent = path.parent();
        auto current = path.current();
        LOG_DEBUG(logger, "register_path: {}", path.debugString());

        auto storage = DatabaseCatalog::instance().tryGetTable(current, init_context);
        if (!storage)
        {
            if (current == init_table_id)
                throw Exception(
                    ErrorCodes::UNKNOWN_TABLE,
                    "Target table '{}' doesn't exists.",
                    init_table_id);

            if (parent == init_table_id)
                throw Exception(
                    ErrorCodes::UNKNOWN_TABLE,
                    "Target table '{}' of view '{}' doesn't exists.",
                    current, init_table_id);

            if (parent)
            {
                if (init_context->getSettingsRef()[Setting::ignore_materialized_views_with_dropped_target_table])
                    return false;

                throw Exception(
                    ErrorCodes::UNKNOWN_TABLE,
                    "Target table '{}' of view '{}' doesn't exists. To ignore this view use setting "
                    "ignore_materialized_views_with_dropped_target_table",
                    current, parent);
            }
        }

        auto lock = storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (lock == nullptr)
        {
            // In case the materialized view is dropped/detached at this point, we register a warning and ignore it
            assert(storage->is_dropped || storage->is_detached);
            LOG_WARNING(getLogger("ViewsManager"), "Trying to access table {} but it doesn't exist", current);
            return false;
        }

        auto metadata = storage->getInMemoryMetadataPtr();

        auto parent_select_context = init_context; // select_contexts.at(parent.view_id);
        auto select_context = metadata->getSQLSecurityOverriddenContext(parent_select_context);
        select_context->setQueryAccessInfo(parent_select_context->getQueryAccessInfoPtr());
        // Processing of blocks for MVs is done block by block, and there will
        // be no parallel reading after (plus it is not a costless operation)
        select_context->setSetting("parallelize_output_from_storages", Field{false});

        auto insert_context = Context::createCopy(select_context);
        insert_context->setQueryAccessInfo(parent_select_context->getQueryAccessInfoPtr());
        if (!deduplicate_blocks_in_dependent_materialized_views)
            insert_context->setSetting("insert_deduplicate", Field{false});

        const auto & insert_settings = insert_context->getSettingsRef();
        // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
        if (insert_settings[Setting::min_insert_block_size_rows_for_materialized_views])
            insert_context->setSetting("min_insert_block_size_rows", insert_settings[Setting::min_insert_block_size_rows_for_materialized_views].value);
        if (insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views])
            insert_context->setSetting("min_insert_block_size_bytes", insert_settings[Setting::min_insert_block_size_bytes_for_materialized_views].value);

        storages[current] = storage;
        metadata_snapshots[current] = metadata;
        storage_locks[current] = std::move(lock);


        if (dynamic_cast<StorageMaterializedView *>(storage.get()))
        {
            if (current == init_table_id)
            {
                select_queries[current] = init_query->as<ASTInsertQuery>()->select;

                select_contexts[current] = init_context;
                insert_contexts[current] = init_context;

                input_headers[current] = init_header;
                select_headers[current] = init_header;
                output_headers[current] = metadata->getSampleBlock();

                thread_groups[current] = CurrentThread::getGroup();

                view_types[current] = QueryViewsLogElement::ViewType::MATERIALIZED;

                LOG_DEBUG(logger, "register_path: dependency {} -> X", current);
                dependent_views[current] = {};

                // root is filled at next call register_path
                return true;
            }

            const auto & select_table_id = metadata->getSelectQuery().select_table_id;
            if (select_table_id != path.parent())
            {
                /// It may happen if materialize view query was changed and it doesn't depend on this source table anymore.
                /// See setting `allow_experimental_alter_materialized_view_structure`
                LOG_DEBUG(logger, "Table '{}' is not a source for view '{}' anymore, current source is '{}'",
                    path.parent(), current, select_table_id);
                return false;
            }

            auto select_query = metadata->getSelectQuery().inner_query;

            Block select_header;
            // Get list of columns we get from select query.
            if (select_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                select_header = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query, select_context);
            else
                select_header = InterpreterSelectQuery(select_query, select_context, SelectQueryOptions()).getSampleBlock();

            select_queries[current] = select_query;
            input_headers[current] = output_headers.at(path.prevParent());
            select_headers[current] = select_header;
            select_contexts[current] = select_context;
            insert_contexts[current] = insert_context;
            // output_headers is filled at next call register_path

            source_tables[current] = parent;

            thread_groups[current] = ThreadGroup::createForMaterializedView();

            view_types[current] = QueryViewsLogElement::ViewType::MATERIALIZED;

            dependent_views[current] = {};

            if (init_context->hasQueryContext())
            {
                init_context->getQueryContext()->addViewAccessInfo(current.getFullTableName());
            }

            return true;
        }
        else if (auto * live_view = dynamic_cast<StorageLiveView *>(init_storage.get()))
        {
            if (current == init_table_id)
            {
                select_queries[current] = init_query->as<ASTInsertQuery>()->select;
                select_contexts[current] = init_context;
                insert_contexts[current] = init_context;
                input_headers[current] = init_header;
                thread_groups[current] = CurrentThread::getGroup();
                view_types[current] = QueryViewsLogElement::ViewType::LIVE;
                root = {init_table_id, init_table_id};
                dependent_views[current] = {};
                return true;
            }

            select_queries[current] = live_view->getInnerQuery();
            input_headers[current] = output_headers.at(path.prevParent());
            select_contexts[current] = select_context;
            insert_contexts[current] = insert_context;
            thread_groups[current] = ThreadGroup::createForMaterializedView();
            view_types[current] = QueryViewsLogElement::ViewType::LIVE;

            if (init_context->hasQueryContext())
            {
                init_context->getQueryContext()->addViewAccessInfo(init_table_id.getFullTableName());
                init_context->getQueryContext()->addQueryAccessInfo(init_table_id, /*column_names=*/ {});
            }

            dependent_views[path.prevParent()].push_back(current);

            return true;
        }
        else if (auto * window_view = dynamic_cast<StorageWindowView *>(init_storage.get()))
        {
            if (current == init_table_id)
            {
                select_queries[current] = init_query->as<ASTInsertQuery>()->select;
                select_contexts[current] = init_context;
                insert_contexts[current] = init_context;
                input_headers[current] = init_header;
                thread_groups[current] = CurrentThread::getGroup();
                view_types[current] = QueryViewsLogElement::ViewType::LIVE;
                root = {init_table_id, init_table_id};
                dependent_views[current] = {};
                return true;
            }

            select_queries[current] = window_view->getMergeableQuery();
            select_contexts[current] = select_context;
            insert_contexts[current] = insert_context;
            input_headers[current] = output_headers.at(path.prevParent());
            thread_groups[current] = ThreadGroup::createForMaterializedView();
            view_types[current] = QueryViewsLogElement::ViewType::WINDOW;

            if (init_context->hasQueryContext())
            {
                init_context->getQueryContext()->addViewAccessInfo(init_table_id.getFullTableName());
                init_context->getQueryContext()->addQueryAccessInfo(init_table_id, /*column_names=*/ {});
            }

            dependent_views[path.prevParent()].push_back(current);

            return true;
        }
        else
        {
            inner_tables[parent] = current;

            if (init_context->hasQueryContext())
            {
                init_context->getQueryContext()->addQueryAccessInfo(current, /*column_names=*/ {});
            }

            if (current == init_table_id)
            {
                select_queries[{}] = init_query->as<ASTInsertQuery>()->select;

                select_contexts[{}] = init_context;
                insert_contexts[{}] = init_context;

                input_headers[{}] = init_header;
                select_headers[{}] = init_header;
                output_headers[{}] = metadata->getSampleBlock();

                thread_groups[{}] = CurrentThread::getGroup();

                view_types[{}] = QueryViewsLogElement::ViewType::DEFAULT;

                dependent_views[{}] = {};

                root = {{}, init_table_id};

                return true;
            }
            else if (parent == init_table_id)
            {
                root = {{init_table_id}, current};
            }

            const auto & view_id = path.parent();
            output_headers[view_id] = metadata->getSampleBlock();

            // TODO: remove sql_security_type check after we turn `ignore_empty_sql_security_in_create_view_query=false`
            auto view_storage = storages.at(view_id);
            auto * m_view = dynamic_cast<StorageMaterializedView *>(view_storage.get());
            chassert(m_view);
            bool check_access = !m_view->hasInnerTable() && m_view->getInMemoryMetadataPtr()->sql_security_type;
            if (check_access)
            {
                LOG_DEBUG(logger, "call checkAccess");
                insert_contexts.at(view_id)->checkAccess(AccessType::INSERT, current, metadata->getSampleBlock().getNames());
            }

            dependent_views[path.prevPrevParent()].push_back(view_id);

            return true;
        }
    };

    VisitedPath path;

    std::function<void(StorageIDPrivate)> expand = [&] (StorageIDPrivate id)
    {
        path.pushBack(id);
        SCOPE_EXIT( path.popBack(); );

        if (!register_path(path))
            return;

        auto storage = storages.at(id);
        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
        {
            expand(materialized_view->getTargetTableId());
        }
        else if (dynamic_cast<StorageLiveView*>(storage.get()))
        {
            // no op
        }
        else if (dynamic_cast<StorageWindowView*>(storage.get()))
        {
            // no op
        }
        else
        {
            for (auto & child : DatabaseCatalog::instance().getDependentViews(id))
            {
                expand(child);
            }
        }
    };

    expand(init_table_id);

    chassert(path.empty());
    LOG_DEBUG(logger, "buildRelaitions2: {}, root is ({}, {})", init_table_id, root.view_id, root.inner_id);
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


Chain ViewsManager::createSelect(StorageIDPrivate view_id) const
{
    LOG_DEBUG(logger, "createSelect: {}", view_id);

    chassert(view_id != init_table_id);

    if (!inner_tables.contains(view_id))
    {
        LOG_DEBUG(logger, "createSelect: no innertable for {}", view_id);
        return {};
    }

    Chain result;

    auto storage = storages.at(view_id);
    auto select_query = select_queries.at(view_id);
    auto select_context = select_contexts.at(view_id);
    auto insert_context = insert_contexts.at(view_id);
    auto inner_table_id = inner_tables.at(view_id);
    auto inner_table_storage = storages.at(inner_table_id);
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
    result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right after Inner query", select_header));
#endif

    auto source_table_id = source_tables.at(view_id);
    auto input_header = input_headers.at(view_id);

    if (select_context->getSettingsRef()[Setting::use_async_executor_for_materialized_views])
    {
        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingAsyncPipelineExecutor>>(
            input_header, select_header,
            select_query,
            source_table_id, storages.at(source_table_id), metadata_snapshots.at(source_table_id),
            view_id, storage, metadata_snapshots.at(view_id),
            select_context);

        executing_inner_query->setRuntimeData(thread_groups.at(view_id));

        result.addSource(std::move(executing_inner_query));
    }
    else
    {
        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingPipelineExecutor>>(
            input_header, select_header,
            select_query,
            source_table_id, storages.at(source_table_id), metadata_snapshots.at(source_table_id),
            view_id, storage, metadata_snapshots.at(view_id),
            select_context);

        executing_inner_query->setRuntimeData(thread_groups.at(view_id));

        result.addSource(std::move(executing_inner_query));
    }

#ifdef DEBUG_OR_SANITIZER_BUILD
    result.addSource(std::make_shared<DeduplicationToken::CheckTokenTransform>("Right before Inner query", input_header));
#endif

    LOG_DEBUG(logger, "createSelect: {}, input {}, output {}", view_id.getNameForLogs(), result.getInputHeader().dumpStructure(), result.getOutputHeader().dumpStructure());

    return result;
}


Chain ViewsManager::createPreSink(StorageIDPrivate view_id) const
{
    LOG_DEBUG(logger, "createPreSink: {}", view_id);

    /// We create a pipeline of several streams, into which we will write data.
    Chain chain;

    auto select_context = select_contexts.at(view_id);
    auto insert_context = insert_contexts.at(view_id);

    auto select_header = select_headers.at(view_id);

    auto inner_id = inner_tables.at(view_id);
    LOG_DEBUG(logger, "createPreSink: {}, inner id {}", view_id, inner_id);

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

    LOG_DEBUG(logger, "createPreSink: {}, transformed header add default {}", view_id, ExpressionTransform::transformHeader(select_header, adding_missing_defaults_actions->getActionsDAG()).dumpStructure());

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    chain.addSink(std::make_shared<ConvertingTransform>(select_header, adding_missing_defaults_actions));

    auto converting = ActionsDAG::makeConvertingActions(
        chain.getOutputHeader().getColumnsWithTypeAndName(),
        inner_storage_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto convert_action = std::make_shared<ExpressionActions>(std::move(converting));

    LOG_DEBUG(logger, "createPreSink: {}, transformed header cast types {}", view_id, ExpressionTransform::transformHeader(chain.getOutputHeader(), convert_action->getActionsDAG()).dumpStructure());

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

    LOG_DEBUG(logger, "createPreSink: {}, iinner_storage_header {}", view_id, inner_storage_header.dumpStructure());

    auto counting = std::make_shared<CountingTransform>(inner_storage_header, insert_context->getQuota());
    counting->setProcessListElement(insert_context->getProcessListElement());
    counting->setProgressCallback(insert_context->getProgressCallback());
    counting->setRuntimeData(thread_groups.at(view_id));
    chain.addSink(std::move(counting));

    LOG_DEBUG(logger, "createPreSink: {}, input {}, output {}", view_id, chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createSink(StorageIDPrivate view_id) const
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
            auto sink = std::make_shared<PushingToLiveViewSink>(select_headers.at(view_id), *live_view, insert_contexts.at(view_id));
            sink->setRuntimeData(thread_groups.at(view_id));
            sink->setViewManager(shared_from_this());
            chain.addSource(std::move(sink));
        }
        else if (auto * window_view = dynamic_cast<StorageWindowView *>(view_storage.get()))
        {
            auto sink = std::make_shared<PushingToWindowViewSink>(window_view->getInputHeader(), *window_view, insert_contexts.at(view_id));
            sink->setRuntimeData(thread_groups.at(view_id));
            sink->setViewManager(shared_from_this());
            chain.addSource(std::move(sink));
        }
        else if (dynamic_cast<StorageMaterializedView *>(view_storage.get()))
        {
            LOG_DEBUG(logger, "createSink: {}, for StorageMaterializedView", view_id.getNameForLogs());

            auto sink = view_storage->write(select_queries.at(view_id), metadata_snapshots.at(view_id), insert_contexts.at(view_id), /*async_insert*/ false);

            LOG_DEBUG(logger, "createSink: {}, sink structure: {}", view_id, sink->getHeader().dumpStructure());

            sink->setRuntimeData(thread_groups.at(view_id));
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
        sink->setRuntimeData(thread_groups.at(view_id));
        sink->setViewManager(shared_from_this());
        chain.addSource(std::move(sink));
    }

    chain.addSink(std::make_shared<DeduplicationToken::DefineSourceWithChunkHashTransform>(chain.getOutputHeader()));

    LOG_DEBUG(logger, "createSink: {} ({}) input {}, output {}", view_id, inner_id, chain.getInputHeader().dumpStructure(), chain.getOutputHeader().dumpStructure());

    return chain;
}


Chain ViewsManager::createPostSink(StorageIDPrivate view_id, size_t level) const
{
    auto inner_table = inner_tables.at(view_id);
    LOG_DEBUG(logger, "createPostSink: {} ({})", view_id, inner_table);

    auto & dependent_views_ids = dependent_views.at(view_id);
    LOG_DEBUG(logger, "createPostSink: {} ({}) dependencies {}" , view_id, inner_table, dependent_views_ids.size());
    if (dependent_views_ids.empty())
        return {};

    std::vector<Chain> view_chains;
    view_chains.reserve(dependent_views_ids.size());

    std::vector<Block> output_view_chains_headers;
    output_view_chains_headers.reserve(dependent_views_ids.size());

    for (auto & child_view_id : dependent_views_ids)
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


    auto copying_data = std::make_shared<CopyTransform>(output_headers.at(view_id), dependent_views_ids.size());
    auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(output_view_chains_headers), std::move(dependent_views_ids), shared_from_this());
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


String getCleanQueryAst(const ASTPtr q, ContextPtr context)
{
    String res = serializeAST(*q);
    if (auto masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(res);

    res = res.substr(0, context->getSettingsRef()[Setting::log_queries_cut_to_length]);

    return res;
}


void ViewsManager::logQueryView(StorageID view_id, std::exception_ptr exception) const
{
    LOG_DEBUG(logger, "logQueryView {}", view_id);

    const auto & settings = init_context->getSettingsRef();
    auto event_status = exception ? QueryViewsLogElement::ViewStatus::EXCEPTION_WHILE_PROCESSING : QueryViewsLogElement::ViewStatus::QUERY_FINISH;

    if (event_status < settings[Setting::log_queries_min_type])
        return;

    if (!view_id || !settings[Setting::log_queries] || !settings[Setting::log_query_views])
        return;

    auto thread_group = thread_groups.at(view_id);
    if (!thread_group)
        return;

    UInt64 elapsed_ms = thread_group->getThreadsTotalElapsedMs();

    UInt64 min_query_duration = settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds();
    if (min_query_duration && elapsed_ms <= min_query_duration)
        return;

    QueryViewsLogElement element;

    auto event_time = std::chrono::system_clock::now();
    element.event_time = timeInSeconds(event_time);
    element.event_time_microseconds = timeInMicroseconds(event_time);

    element.view_duration_ms = elapsed_ms;
    element.initial_query_id = CurrentThread::getQueryId();

    element.view_name = view_id.getFullTableName();
    element.view_uuid = view_id.uuid;
    element.view_type = view_types.at(view_id);
    element.view_query = getCleanQueryAst(select_queries.at(view_id), select_contexts.at(view_id));
    element.view_target = inner_tables.at(view_id).getFullTableName();

    element.peak_memory_usage = thread_group->memory_tracker.getPeak() > 0 ? thread_group->memory_tracker.getPeak() : 0;

    auto profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());

    element.read_rows = (*profile_counters)[ProfileEvents::SelectedRows];
    element.read_bytes = (*profile_counters)[ProfileEvents::SelectedBytes];
    element.written_rows = (*profile_counters)[ProfileEvents::InsertedRows];
    element.written_bytes = (*profile_counters)[ProfileEvents::InsertedBytes];

    if (settings[Setting::log_profile_events] != 0)
        element.profile_counters = std::move(profile_counters);

    element.status = event_status;
    element.exception_code = 0;
    if (exception)
    {
        element.exception_code = getExceptionErrorCode(exception);
        element.exception = getExceptionMessage(exception, false);
        if (settings[Setting::calculate_text_stack_trace])
            element.stack_trace = getExceptionStackTraceString(exception);
    }

    try
    {
        auto views_log = init_context->getQueryViewsLog();
        if (!views_log)
            return;

        views_log->add(std::move(element));
        LOG_DEBUG(logger, "logQueryView {} added", view_id);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
