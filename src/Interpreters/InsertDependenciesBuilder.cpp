#include <Interpreters/InsertDependenciesBuilder.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ApplySquashingTransform.h>
#include <Processors/Transforms/RemovingSparseTransform.h>
#include <Processors/Transforms/RemovingReplicatedColumnsTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
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
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Interpreters/Context.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/CheckConstraintsTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/NestedElementsValidationTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Chunk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Formats/FormatFactory.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/Pipe.h>

#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadStatus.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/logger_useful.h>
#include <Core/DeduplicateInsert.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/Block.h>
#include <Core/LogsLevel.h>
#include <Core/Settings.h>

#include <base/UUID.h>
#include <base/scope_guard.h>
#include <base/defines.h>

#include <atomic>
#include <cassert>
#include <exception>
#include <memory>
#include <unordered_map>
#include <vector>
#include <iterator>

namespace ProfileEvents
{
    extern const Event InsertQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
    extern const Event InsertedRows;
    extern const Event InsertedBytes;
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
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsBool insert_null_as_default;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsBool log_query_views;
    extern const SettingsBool log_queries;
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsBool log_profile_events;
    extern const SettingsBool calculate_text_stack_trace;
    extern const SettingsBool use_async_executor_for_materialized_views;
    extern const SettingsBool materialized_views_ignore_errors;
    extern const SettingsBool materialized_views_squash_parallel_inserts;
    extern const SettingsBool parallel_view_processing;
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
    extern const int DEPENDENCIES_NOT_FOUND;
}


class ViewErrorsRegistry
{
    template <class T>
    class SetOnce
    {
    private:
        std::atomic_flag is_set;
        T value;
    public:
        bool set(T value_)
        {
            if (is_set.test_and_set())
                return false;

            value = value_;
            return true;
        }
        bool isSet() const
        {
            return is_set.test();
        }
        T get() const
        {
            if (is_set.test())
                return value;
            return T{};
        }
    };

public:
    class ViewErrors
    {
        std::atomic_size_t finalizers_count = 0;

    public:
        SetOnce<std::exception_ptr> external_exception;
        SetOnce<std::exception_ptr> current_exception;

        void registerFinalizer()
        {
            finalizers_count.fetch_add(1);
        }

        bool needLogQueryView()
        {
            return finalizers_count.fetch_sub(1) == 1;
        }
    };

private:
    using MapIdViewExceptions = std::unordered_map<StorageIDMaybeEmpty, ViewErrors, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    MapIdViewExceptions view_errors;

public:
    SetOnce<std::exception_ptr> global_exception;

    void init(const StorageIDMaybeEmpty & view_id)
    {
        view_errors.try_emplace(view_id);
    }

    const ViewErrors & getErrors(const StorageIDMaybeEmpty & view_id) const
    {
        return view_errors.at(view_id);
    }

    ViewErrors & getErrors(const StorageIDMaybeEmpty & view_id)
    {
        return view_errors.at(view_id);
    }

    std::exception_ptr getFinalError(const StorageIDMaybeEmpty & view_id, bool ignore_global) const
    {
        const auto & errors = getErrors(view_id);
        if (auto e = errors.current_exception.get())
            return e;
        if (auto e = errors.external_exception.get())
            return e;
        if (!ignore_global)
            return global_exception.get();
        return nullptr;
    }
};


static std::exception_ptr addStorageToException(std::exception_ptr ptr, const StorageID & storage)
{
    try
    {
        std::rethrow_exception(ptr);
    }
    catch (DB::Exception & exception)
    {
        // we have to make a copy of exception here,
        // because the original exception is multiplied by CopyTransform
        // it should not be modified anywhere to avoid concurrant modification
        auto patch = DB::Exception(exception);
        patch.addMessage("while pushing to view {}", storage.getNameForLogs());
        return std::make_exception_ptr(std::move(patch));
    }
    catch (...)
    {
        return ptr;
    }
}


class PushingToWindowViewSink final : public SinkToStorage
{
public:
    PushingToWindowViewSink(SharedHeader header, StorageWindowView & window_view_, ContextPtr context_)
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


class BeginingViewsTransform final : public ISimpleTransform
{
public:
    explicit BeginingViewsTransform(SharedHeader header)
        : ISimpleTransform(header, header, false)
    {}

    String getName() const override { return "BeginingViewsTransform"; }

    void transform(Chunk &) override { /* no op */ }

    struct ExternalException
    {
        std::exception_ptr origin_exception;
    };

    void transform(std::exception_ptr & e) override
    {
        e = std::make_exception_ptr(ExternalException{e});
    }
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
    FinalizingViewsTransform(std::vector<Block> headers, std::vector<StorageID> views, InsertDependenciesBuilder::ConstPtr insert_dependencies_, ViewErrorsRegistryPtr views_error_registry_)
        : IProcessor(initPorts(std::move(headers)), {Block()})
        , output(outputs.front())
        , insert_dependencies(insert_dependencies_)
        , views_error_registry(views_error_registry_)
    {
        chassert(inputs.size() == views.size());

        statuses.reserve(views.size());
        for (auto & view_id : views)
        {
            views_error_registry->getErrors(view_id).registerFinalizer();

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

        size_t num_finished = 0;
        size_t i = 0;
        for (auto & input : inputs)
        {
            auto pos = i;
            ++i;

            auto & status = statuses[pos];

            if (input.isFinished())
            {
                ++num_finished;
                status.is_finished = true;
                continue;
            }

            input.setNeeded();
            if (!input.hasData())
                continue;

            auto data = input.pullData();

            if (!data.exception)
                continue;

            auto & view_errors = views_error_registry->getErrors(status.view_id);

            auto [is_external, original_exception] = unwrapExternalException(data.exception);

            if (is_external)
            {
                view_errors.external_exception.set(original_exception);

                output.pushException(original_exception);
                return Status::PortFull;
            }

            auto exception_with_storage = addStorageToException(original_exception, status.view_id);

            views_error_registry->global_exception.set(exception_with_storage);
            if (!view_errors.current_exception.set(exception_with_storage))
                continue;

            if (!insert_dependencies->materialized_views_ignore_errors)
            {
                output.pushException(exception_with_storage);
                return Status::PortFull;
            }

            tryLogException(
                exception_with_storage,
                getLogger("FinalizingViewsTransform"),
                "Cannot push to the storage. Error is ignored because the setting materialized_views_ignore_errors is enabled.",
                LogsLevel::warning);

            input.setNeeded();
            return Status::NeedData;
        }

        if (num_finished == inputs.size())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::NeedData;
    }

    void writeViewLogs()
    {
        for (auto & status : statuses)
        {
            auto & errors = views_error_registry->getErrors(status.view_id);

            if (!errors.needLogQueryView())
                continue;

            if (insert_dependencies->materialized_views_ignore_errors)
            {
                if (status.is_finished)
                {
                    insert_dependencies->logQueryView(status.view_id, views_error_registry->getFinalError(status.view_id, /*ignore_global*/ true));
                }
                else
                {
                    insert_dependencies->logQueryView(status.view_id, views_error_registry->getFinalError(status.view_id, /*ignore_global*/ false));
                }
            }
            else
            {
                insert_dependencies->logQueryView(status.view_id, views_error_registry->getFinalError(status.view_id, /*ignore_global*/ false));
            }
        }
        statuses.clear();
    }

    void work() override { /* no op */ }

    ~FinalizingViewsTransform() override
    {
        try
        {
            writeViewLogs();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    struct ViewStatus
    {
        explicit ViewStatus(StorageID view_id_)
            : view_id(std::move(view_id_))
        {}

        StorageID view_id;
        bool is_finished = false;
    };

    static std::pair<bool, std::exception_ptr> unwrapExternalException(std::exception_ptr & e)
    {
        try
        {
            std::rethrow_exception(e);
        }
        catch (BeginingViewsTransform::ExternalException & wrapped)
        {
            return {true, wrapped.origin_exception};
        }
        catch (...)
        {
            return {false, e};
        }
    }

    OutputPort & output;

    InsertDependenciesBuilder::ConstPtr insert_dependencies;
    ViewErrorsRegistryPtr views_error_registry;
    std::vector<ViewStatus> statuses;
};


DB::ConstraintsDescription buildConstraints(StorageMetadataPtr metadata, StoragePtr storage)
{
    auto constraints = metadata->getConstraints();

    auto storage_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
    if (storage_merge_tree
        && (storage_merge_tree->merging_params.mode == MergeTreeData::MergingParams::Collapsing
            || storage_merge_tree->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        && (*storage_merge_tree->getSettings())[MergeTreeSetting::add_implicit_sign_column_constraint_for_collapsing_engine])
    {
        auto sign_column_check_constraint = make_intrusive<ASTConstraintDeclaration>();
        sign_column_check_constraint->name = "_implicit_sign_column_constraint";
        sign_column_check_constraint->type = ASTConstraintDeclaration::Type::CHECK;

        Array valid_values_array;
        valid_values_array.emplace_back(-1);
        valid_values_array.emplace_back(1);

        auto valid_values_ast = make_intrusive<ASTLiteral>(std::move(valid_values_array));
        auto sign_column_ast = make_intrusive<ASTIdentifier>(storage_merge_tree->merging_params.sign_column);
        sign_column_check_constraint->set(sign_column_check_constraint->expr, makeASTOperator("in", std::move(sign_column_ast), std::move(valid_values_ast)));

        auto constraints_ast = constraints.getConstraints();
        constraints_ast.push_back(std::move(sign_column_check_constraint));
        constraints = ConstraintsDescription(constraints_ast);
    }

    return constraints;
}


/// For source chunk, execute view query over it.
template <typename Executor>
class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(
        SharedHeader input_header, SharedHeader output_header,
        ASTPtr select_query_,
        StorageID source_id_, StoragePtr source_storage_, StorageMetadataPtr source_metadata_,
        StorageID view_id_, StoragePtr view_storage_, StorageMetadataPtr view_metadata_,
        StorageID inner_id_, StoragePtr inner_storage_, StorageMetadataPtr inner_metadata_,
        ContextPtr context_)
        : ExceptionKeepingTransform(input_header, output_header)
        , select_query(select_query_)
        , source_id(source_id_)
        , source_metadata(source_metadata_)
        , source_storage(source_storage_)
        , view_id(view_id_)
        , view_metadata(view_metadata_)
        , view_storage(view_storage_)
        , inner_id(inner_id_)
        , inner_metadata(inner_metadata_)
        , inner_storage(inner_storage_)
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
    StorageMetadataPtr source_metadata;
    StoragePtr source_storage;
    StorageID view_id;
    StorageMetadataPtr view_metadata;
    StoragePtr view_storage;
    StorageID inner_id;
    StorageMetadataPtr inner_metadata;
    StoragePtr inner_storage;
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
            source_metadata->getColumns(),
            std::move(data_block),
            *source_storage->getVirtualsPtr()));

        QueryPipelineBuilder pipeline;

        if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter(
                select_query, local_context, SelectQueryOptions().ignoreAccessCheck(), local_context->getViewSource());
            pipeline = interpreter.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectQuery interpreter(select_query, local_context, SelectQueryOptions().ignoreAccessCheck());
            pipeline = interpreter.buildQueryPipeline();
        }
        pipeline.resize(1);
        pipeline.dropTotalsAndExtremes();

        bool insert_null_as_default = false;

        auto construct_columns_to_convert = [] (const ColumnsWithTypeAndName & src, const ColumnsWithTypeAndName & dst)
        {
            NameToIndexMap name_to_index_dst_map;
            for (size_t i = 0; i < dst.size(); ++i)
                name_to_index_dst_map[dst[i].name] = i;

            ColumnsWithTypeAndName result;
            for (const auto & column : src)
            {
                if (name_to_index_dst_map.contains(column.name))
                    result.push_back(dst[name_to_index_dst_map[column.name]]);
                else
                    result.push_back(column);
            }
            return result;
        };

        auto build_conversion = [&] (const Block & input, StorageMetadataPtr result_metadata)
        {
            /// Some time input header contains lesser columns than result header,
            /// and we need to convert types of input columns to match result header.
            /// But at converting stage we have to only convert types of columns that are present in input header,
            /// so we construct a list of columns to convert which contains only columns that are present in input header.
            /// The additional columns that are present in result header are added later by `addMissingDefaults` function.
            /// Also adding_missing_defaults_dag would require access to the subcolumns, so we extract them before adding defaults.
            /// As a result, the order of actions is mattered and it is:
            /// - convert types of columns that are present in input header
            /// - extract subcolumns that are require for adding defaults
            /// - add missing defaults for columns that are present in result header but not in input header
            auto to_convert = construct_columns_to_convert(input.getColumnsWithTypeAndName(), result_metadata->getSampleBlock().getColumnsWithTypeAndName());

            auto converting_types_dag = ActionsDAG::makeConvertingActions(
                input.getColumnsWithTypeAndName(),
                to_convert,
                ActionsDAG::MatchColumnsMode::Name,
                local_context);

            auto adding_missing_defaults_dag = addMissingDefaults(
                Block(to_convert),
                result_metadata->getSampleBlock().getNamesAndTypesList(),
                result_metadata->getColumns(),
                local_context,
                insert_null_as_default);

            auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(
                Block(to_convert),
                adding_missing_defaults_dag.getRequiredColumnsNames(),
                local_context);

            return ActionsDAG::merge(
                std::move(converting_types_dag),
                ActionsDAG::merge(
                     std::move(extracting_subcolumns_dag), std::move(adding_missing_defaults_dag)));
        };

        pipeline.addTransform(std::make_shared<ExpressionTransform>(
            pipeline.getSharedHeader(),
            std::make_shared<ExpressionActions>(
                build_conversion(
                    pipeline.getHeader(),
                    inner_metadata))));

        inner_metadata->check(pipeline.getHeader());

        /// Squashing is needed here because the materialized view query can generate a lot of blocks
        /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
        /// and two-level aggregation is triggered).
        pipeline.addTransform(std::make_shared<SquashingTransform>(
            pipeline.getSharedHeader(),
            context->getSettingsRef()[Setting::min_insert_block_size_rows],
            context->getSettingsRef()[Setting::min_insert_block_size_bytes]));

        pipeline.addTransform(std::make_shared<RestoreChunkInfosTransform>(std::move(chunk_infos), pipeline.getSharedHeader()));

        pipeline.addTransform(std::make_shared<UpdateDeduplicationInfoWithViewIDTransform>(view_id, pipeline.getSharedHeader()));

        return QueryPipelineBuilder::getPipeline(std::move(pipeline));
    }

    std::optional<State> state;
};


InsertDependenciesBuilder::InsertDependenciesBuilder(
    StoragePtr table, ASTPtr query, SharedHeader insert_header,
    bool async_insert_, bool skip_destination_table_, size_t max_insert_threads,
    ContextPtr context)
    : init_table_id(table->getStorageID())
    , init_storage(table)
    , init_query(query)
    , init_header(std::move(insert_header))
    , init_context(context)
    , async_insert(async_insert_)
    , skip_destination_table(skip_destination_table_)
    , views_error_registry(std::make_shared<ViewErrorsRegistry>())
    , logger(getLogger("InsertDependenciesBuilder"))
{
    const auto & settings = init_context->getSettingsRef();

    const ASTInsertQuery * as_insert_query = init_query->as<ASTInsertQuery>();
    insert_null_as_default = as_insert_query && as_insert_query->select && settings[Setting::insert_null_as_default];

    deduplicate_blocks = isDeduplicationEnabledForInsert(async_insert, settings);
    LOG_DEBUG(logger, "deduplicate_blocks : {}", deduplicate_blocks);
    deduplicate_blocks_in_dependent_materialized_views = deduplicate_blocks && settings[Setting::deduplicate_blocks_in_dependent_materialized_views];
    materialized_views_ignore_errors = settings[Setting::materialized_views_ignore_errors];
    /// Squashing from multiple streams breaks deduplication for now so the optimization will be disabled
    /// if deduplication for MVs is enabled
    /// TODO: for sync insert we could squash at the first level
    squash_parallel_inserts = !deduplicate_blocks_in_dependent_materialized_views && settings[Setting::materialized_views_squash_parallel_inserts];
    ignore_materialized_views_with_dropped_target_table = settings[Setting::ignore_materialized_views_with_dropped_target_table];

    collectAllDependencies();

    auto all_sinks_support_parallel_insert = std::ranges::all_of(storages, [&] (auto storage)
        { return isView(storage.first) || storage.second->supportsParallelInsert();});
    if (all_sinks_support_parallel_insert && (settings[Setting::parallel_view_processing] || !isViewsInvolved()))
        sink_stream_size = max_insert_threads;
}

namespace
{

struct SquashingTransformContext
{
    size_t num_squashing_transforms = 0;
    bool squashing_transform_added = false;
    OutputPorts::iterator output_it;
    InputPorts::iterator input_it;
};

}

std::vector<Chain> InsertDependenciesBuilder::createChainWithDependenciesForAllStreams() const
{
    std::vector<Chain> insert_chains;
    std::vector<SquashingProcessorsMap> squashing_processor_maps;
    std::unordered_map<
        StorageIDMaybeEmpty,
        SquashingTransformContext,
        StorageID::DatabaseAndTableNameHash,
        StorageID::DatabaseAndTableNameEqual>
        views_to_squashing_context;

    insert_chains.reserve(sink_stream_size);

    if (squash_parallel_inserts)
        squashing_processor_maps.reserve(sink_stream_size);

    bool has_squashing_transforms = false;
    for (size_t i = 0; i < sink_stream_size; ++i)
    {
        insert_chains.emplace_back(createChainWithDependencies());
        if (squash_parallel_inserts)
        {
            /// Collect total amount of squashing transforms for each destination table so we can create
            /// shrink/expand processors with enough input/output ports.
            /// Collect all squashing processors for each chain so we don't need to recreate same chain for each destination table.
            for (const auto & [view_id, apply_squashing_processors] : squashing_processors)
            {
                has_squashing_transforms |= !apply_squashing_processors.empty();
                views_to_squashing_context[view_id].num_squashing_transforms += apply_squashing_processors.size();
            }
            squashing_processor_maps.emplace_back(std::move(squashing_processors));
        }
    }

    if (!squash_parallel_inserts || !has_squashing_transforms)
        return insert_chains;

    /// We need to extract processors from chain and modify it.
    /// For each destination table we do the following:
    ///   - In first chain with it, we will add new chain of processors consisting of
    ///     shrink processor -> PlanSquashingTransform -> expand processor
    ///     We also connect the output and input ports of shrink and expand processors.
    ///   - For every other chain with the same destination table we won't add new processors,
    ///     we will connect processors created in first chain using the input and output port
    ///     of shrink and expand processors.
    /// We can create the chains only after processing all other chains because all ports need to be connected.
    std::vector<std::pair<std::list<ProcessorPtr>, QueryPlanResourceHolder>> result_data;
    result_data.reserve(insert_chains.size());

    for (auto && [chain, squashing_processors_for_chain] : std::views::zip(insert_chains, squashing_processor_maps))
    {
        auto resources = chain.detachResources();
        auto processor_list = std::move(chain.getProcessors());

        for (const auto & [view_id, apply_squashing_processors] : squashing_processors_for_chain)
        {
            auto & squashing_context = views_to_squashing_context.at(view_id);
            std::list<ProcessorPtr> squashing_processors_list;
            /// First time we saw this destination table.
            /// Add required processors, connect it and store it's output and input port iterator so other
            /// chains with the same destination table can use it.
            if (!squashing_context.squashing_transform_added)
            {
                const auto & output_header = output_headers.at(view_id);
                const auto & inner_storage = storages.at(view_id);
                auto insert_context = insert_contexts.at(view_id);
                bool table_prefers_large_blocks = inner_storage->prefersLargeBlocks();
                const auto & settings = insert_context->getSettingsRef();

                if (squashing_context.num_squashing_transforms > 1)
                {
                    squashing_processors_list.emplace_back(
                        std::make_shared<ResizeProcessor>(output_header, squashing_context.num_squashing_transforms, 1));
                }

                auto & plan_squashing_transform = squashing_processors_list.emplace_back(
                    std::make_shared<PlanSquashingTransform>(
                        output_header,
                        table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                        table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL));

                if (squashing_context.num_squashing_transforms > 1)
                {
                    auto & shrink_processor = squashing_processors_list.front();
                    auto & expand_processor = squashing_processors_list.emplace_back(
                        std::make_shared<ResizeProcessor>(output_header, 1, squashing_context.num_squashing_transforms));

                    connect(shrink_processor->getOutputs().front(), plan_squashing_transform->getInputs().front());
                    connect(plan_squashing_transform->getOutputs().front(), expand_processor->getInputs().front());
                }

                squashing_context.output_it = squashing_processors_list.back()->getOutputs().begin();
                squashing_context.input_it = squashing_processors_list.front()->getInputs().begin();
            }

            for (const auto & apply_squashing_processor_it : apply_squashing_processors)
            {
                auto before_squashing_processor_it = std::prev(apply_squashing_processor_it);
                connect((*before_squashing_processor_it)->getOutputs().front(), *squashing_context.input_it, true);
                ++squashing_context.input_it;

                connect(*squashing_context.output_it, (*apply_squashing_processor_it)->getInputs().front(), true);
                ++squashing_context.output_it;

                if (!std::exchange(squashing_context.squashing_transform_added, true))
                {
                    chassert(!squashing_processors_list.empty());
                    processor_list.splice(apply_squashing_processor_it, std::move(squashing_processors_list));
                    squashing_processors_list.clear();
                }
            }
        }

        result_data.push_back(std::make_pair(std::move(processor_list), std::move(resources)));
    }

    std::vector<Chain> result_chains;
    result_chains.reserve(result_data.size());

    for (auto & [processor_list, resources] : result_data)
    {
        auto & chain = result_chains.emplace_back(std::move(processor_list));
        chain.attachResources(std::move(resources));
        chain.setNumThreads(init_context->getSettingsRef()[Setting::max_threads]);
        chain.setConcurrencyControl(init_context->getSettingsRef()[Setting::use_concurrency_control]);
    }

    return result_chains;
}


Chain InsertDependenciesBuilder::createRedefineDeduplicationInfoWithDataHashTransformChain() const
{
    const auto & dependent_views_ids = dependent_views.at(root_view);
    if (dependent_views_ids.empty())
        return {};

    auto output_header = output_headers.at(root_view);

    Chain chain;
    chain.addSink(std::make_shared<RedefineDeduplicationInfoWithDataHashTransform>(output_header));
    return chain;
}


Chain InsertDependenciesBuilder::createChainWithDependencies() const
{
    Chain result;

    if (!skip_destination_table)
    {
        result = Chain::concat(std::move(result), createPreSink(root_view));
        result = Chain::concat(std::move(result), createSink(root_view));
    }

    // *Log storages have method `noPushingToViewsOnInserts` returned `true`.
    // When data is inserted to the *Log storages, then it is not supposed to be inserted to the dependent views
    // When *Log storages push data to the dependent views, then `skip_destination_table` is true, data is pushed to the views only, not to the destination table
    if (!init_storage->noPushingToViewsOnInserts() || skip_destination_table)
    {
        result = Chain::concat(std::move(result), createRedefineDeduplicationInfoWithDataHashTransformChain());
        result = Chain::concat(std::move(result), createPostSink(root_view));
    }

    if (skip_destination_table && result.empty())
        throw Exception(ErrorCodes::DEPENDENCIES_NOT_FOUND,
            "Table '{}' doesn't have any dependencies.",
            init_table_id);

    result.setNumThreads(init_context->getSettingsRef()[Setting::max_threads]);
    result.setConcurrencyControl(init_context->getSettingsRef()[Setting::use_concurrency_control]);

    result.addInsertDependenciesBuilder(shared_from_this());

    return result;
}


std::pair<ContextPtr, ContextPtr> InsertDependenciesBuilder::createSelectInsertContext(const DependencyPath & path)
{
    const auto & current = path.current();

    auto parent_select_context = select_contexts.at(path.parent(2));
    auto select_context = metadata_snapshots.at(current)->getSQLSecurityOverriddenContext(parent_select_context);
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

    return {select_context, insert_context};
}


String InsertDependenciesBuilder::debugTree() const
{
    WriteBufferFromOwnString output_buffer;

    DependencyPath path;
    std::function<void(StorageIDMaybeEmpty)> visit = [&](StorageIDMaybeEmpty id)
    {
        path.pushBack(id);
        SCOPE_EXIT({
            path.popBack();
        });

        output_buffer << debugPath(path);

        if (isView(id))
        {
            if (inner_tables.contains(id))
                visit(inner_tables.at(id));
        }
        else
        {
            auto view_id = path.parent(1);
            chassert(isView(view_id));
            for (const auto & child_id: dependent_views.at(view_id))
                visit(child_id);
        }
    };

    visit(root_view);
    return output_buffer.str();
}


String InsertDependenciesBuilder::debugPath(const DependencyPath & path) const
{
    static const String tab = "    ";
    WriteBufferFromOwnString output_buffer;
    output_buffer << "{\n";
    output_buffer << tab << "path " << path.debugInfo() << "\n";

    const auto & current = path.current();

    if (isView(current))
    {
        output_buffer << tab << "it is a view\n";
        output_buffer << tab << "source table: " << (source_tables.contains(current) ? source_tables.at(current).getTableName() : "<not set>") << "\n";
        output_buffer << tab << "select query: " << select_queries.at(current)->formatForLogging() << "\n";

        if (metadata_snapshots.contains(current))
        {
            auto view_metadata = metadata_snapshots.at(current);
            output_buffer << tab << "view table header: " << view_metadata->getSampleBlock().dumpStructure() << "\n";
        }

        auto inner_id = inner_tables.at(current);
        output_buffer << tab << "inner table: " << inner_id.getTableName() << "\n";
        auto inner_metadata = metadata_snapshots.at(inner_id);
        output_buffer << tab << "inner table header: " << inner_metadata->getSampleBlock().dumpStructure() << "\n";


        output_buffer << tab << "input header: " << input_headers.at(current)->dumpStructure() << "\n";
        output_buffer << tab << "output header: " << output_headers.at(current)->dumpStructure() << "\n";
    }
    else
    {
        output_buffer << tab << "it is a table\n";
        output_buffer << tab << "related view: " << fmt::format("{}", path.parent(1)) << "\n";
    }

    output_buffer << "}\n";
    return output_buffer.str();
}


bool InsertDependenciesBuilder::observePath(const DependencyPath & path)
{
    const auto & parent = path.parent(1);
    const auto & current = path.current();

    auto storage = current == init_table_id ? init_storage : DatabaseCatalog::instance().tryGetTable(current, init_context);
    auto lock = storage ? storage->tryLockForShare(init_context->getInitialQueryId(), init_context->getSettingsRef()[Setting::lock_acquire_timeout]) : nullptr;
    if (!lock)
    {
        if (current == init_table_id)
            throw Exception(
                ErrorCodes::UNKNOWN_TABLE,
                "Table '{}' doesn't exists.",
                init_table_id);

        if (isView(parent))
        {
            if (parent == init_table_id)
                throw Exception(
                    ErrorCodes::UNKNOWN_TABLE,
                    "Target table '{}' of view '{}' doesn't exists.",
                    current, init_table_id);

            if (!ignore_materialized_views_with_dropped_target_table)
                throw Exception(
                    ErrorCodes::UNKNOWN_TABLE,
                    "Target table '{}' of view '{}' doesn't exists.",
                    current, init_table_id);

            LOG_INFO(logger, "Trying to access target table '{}' of view '{}' but it doesn't exist", current, parent);
            return false;
        }

        LOG_INFO(logger, "Trying to access view '{}' but it doesn't exist", current);
        return false;
    }

    chassert(storage);
    auto metadata = storage->getInMemoryMetadataPtr();

    storages[current] = storage;
    metadata_snapshots[current] = metadata;
    storage_locks[current] = std::move(lock);

    auto set_defaults_for_root_view = [&] (const StorageIDMaybeEmpty & root_view_, const StorageIDMaybeEmpty & inner_table_)
    {
        const auto select_context = metadata->getSQLSecurityOverriddenContext(init_context);
        const auto insert_context = metadata->getSQLSecurityOverriddenContext(init_context);

        root_view = root_view_;
        inner_tables[root_view] = inner_table_;
        select_queries[root_view] = init_query;
        select_contexts[root_view] = select_context;
        insert_contexts[root_view] = insert_context;
        input_headers[root_view] = init_header;
        /// For background tasks (i.e. Buffer flush) there may not be any group
        if (auto thread_group = CurrentThread::getGroup())
            thread_groups[root_view] = thread_group;
        else
            thread_groups[root_view] = ThreadGroup::createForMaterializedView(init_context);
        views_error_registry->init(root_view);
        dependent_views[root_view] = {};
    };

    if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        if (current == init_table_id)
        {
            set_defaults_for_root_view(init_table_id, materialized_view->getTargetTableId());
            view_types[init_table_id] = QueryViewsLogElement::ViewType::MATERIALIZED;
            return true;
        }

        StorageIDMaybeEmpty select_table_id = metadata->getSelectQuery().select_table_id;
        if (select_table_id != parent)
        {
            /// It may happen if materialize view query was changed and it doesn't depend on this source table anymore.
            /// See setting `allow_experimental_alter_materialized_view_structure`
            LOG_INFO(logger, "Table '{}' is not a source for view '{}' anymore, current source is '{}'",
                parent, current, select_table_id);
            return false;
        }

        inner_tables[current] = materialized_view->getTargetTableId();
        source_tables[current] = parent;
        thread_groups[current] = ThreadGroup::createForMaterializedView(init_context);
        view_types[current] = QueryViewsLogElement::ViewType::MATERIALIZED;
        views_error_registry->init(current);

        select_queries[current] = metadata->getSelectQuery().inner_query;
        input_headers[current] = output_headers.at(path.parent(2));
        // output_headers is filled at next call observePath(inner_table)

        std::tie(select_contexts[current], insert_contexts[current]) = createSelectInsertContext(path);

        dependent_views[current] = {};

        if (init_context->hasQueryContext())
            init_context->getQueryContext()->addViewAccessInfo(current.getFullTableName());

        return true;
    }
    else if (auto * window_view = dynamic_cast<StorageWindowView *>(init_storage.get()))
    {
        if (current == init_table_id)
        {
            set_defaults_for_root_view(init_table_id, init_table_id);
            view_types[init_table_id] = QueryViewsLogElement::ViewType::WINDOW;
            return true;
        }

        inner_tables[current] = current;
        select_queries[current] = window_view->getMergeableQuery();
        input_headers[current] = output_headers.at(path.parent(2));
        thread_groups[current] = ThreadGroup::createForMaterializedView(init_context);
        view_types[current] = QueryViewsLogElement::ViewType::WINDOW;
        views_error_registry->init(current);

        auto parent_select_context = select_contexts.at(path.parent(2));
        auto view_context = metadata->getSQLSecurityOverriddenContext(parent_select_context);
        view_context->setQueryAccessInfo(parent_select_context->getQueryAccessInfoPtr());
        select_contexts[current] = view_context;
        insert_contexts[current] = view_context;

        if (init_context->hasQueryContext())
        {
            init_context->getQueryContext()->addViewAccessInfo(current.getFullTableName());
            init_context->getQueryContext()->addQueryAccessInfo(current, /*column_names=*/ {});
        }

        dependent_views[path.parent(2)].push_back(current);

        return true;
    }
    else
    {
        /// the last case is a regular table
        /// at the first iteration it is the init_table_id most likely
        /// the following iterations will be for inner tables of materialized views

        if (init_context->hasQueryContext())
            init_context->getQueryContext()->addQueryAccessInfo(current, /*column_names=*/ {});

        if (current == init_table_id)
        {
            /// set root_view to `{}`/`StorageID::createEmpty()` and dependent_views[{}] to the init_table_id
            set_defaults_for_root_view({}, init_table_id);
            output_headers[{}] = std::make_shared<const Block>(metadata->getSampleBlock());
            view_types[{}] = QueryViewsLogElement::ViewType::DEFAULT;
            return true;
        }

        const auto & view_id = parent;

        chassert(inner_tables.at(view_id) == current);
        output_headers[view_id] = std::make_shared<const Block>(metadata->getSampleBlock());

        // TODO: remove sql_security_type check after we turn `ignore_empty_sql_security_in_create_view_query=false`
        auto view_storage = storages.at(view_id);
        auto * m_view = dynamic_cast<StorageMaterializedView *>(view_storage.get());
        chassert(m_view);
        bool check_access = !m_view->hasInnerTable() && metadata_snapshots.at(view_id)->sql_security_type;
        if (check_access)
            insert_contexts.at(view_id)->checkAccess(AccessType::INSERT, current, metadata->getSampleBlockInsertable().getNames());

        dependent_views[path.parent(3)].push_back(view_id);

        return true;
    }
}


void InsertDependenciesBuilder::collectAllDependencies()
{
    DependencyPath path;

    std::function<void(StorageIDMaybeEmpty)> expand = [&] (StorageIDMaybeEmpty id)
    {
        path.pushBack(id);
        SCOPE_EXIT({
             path.popBack();
        });

        try
        {
            if (!observePath(path))
                return;
        }
        catch (...)
        {
            if (!materialized_views_ignore_errors)
                throw;

            if (id == init_table_id)
                throw;

            auto view_id = isView(id) ? id : path.parent(1);

            if (view_id == init_table_id)
                throw;

            auto exception = addStorageToException(std::current_exception(), view_id);
            tryLogException(
                exception,
                logger,
                "Cannot push to the storage. Error is ignored because the setting materialized_views_ignore_errors is enabled.",
                LogsLevel::warning);
            logQueryView(view_id, exception, /*before_start*/ true);
            return;
        }

        if (isView(id))
        {
            auto inner_table = inner_tables.at(id);
            if (!inner_table.empty() && inner_table != id)
                expand(inner_table);
        }
        else
        {
            // Destination tables for StorageMaterializedView does not have id in inner_tables
            for (auto & child : DatabaseCatalog::instance().getDependentViews(id))
                expand(child);
        }
    };

    expand(init_table_id);

    chassert(path.empty());

    if (skip_destination_table)
    {
        output_headers[root_view] = init_header;
        for (const auto & child_id : dependent_views.at(root_view))
        {
            input_headers[child_id] = init_header;
        }
    }
}


Chain InsertDependenciesBuilder::createSelect(StorageIDMaybeEmpty view_id) const
{
    chassert(view_id != init_table_id);

    Chain result;

    auto select_query = select_queries.at(view_id);
    auto select_context = select_contexts.at(view_id);
    auto insert_context = insert_contexts.at(view_id);
    auto inner_table_id = inner_tables.at(view_id);
    auto inner_storage = storages.at(inner_table_id);
    auto output_header = output_headers.at(view_id);

    if (squash_parallel_inserts)
    {
        result.addSource(std::make_shared<ApplySquashingTransform>(output_header));
        squashing_processors[view_id].push_back(result.getProcessors().begin());
    }


    auto counting = std::make_shared<CountingTransform>(output_header, insert_context->getQuota());
    counting->setProcessListElement(insert_context->getProcessListElement());
    counting->setProgressCallback(insert_context->getProgressCallback());
    counting->setRuntimeData(thread_groups.at(view_id));
    result.addSource(std::move(counting));

    auto source_table_id = source_tables.at(view_id);
    auto input_header = input_headers.at(view_id);

    if (select_context->getSettingsRef()[Setting::use_async_executor_for_materialized_views])
    {
        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingAsyncPipelineExecutor>>(
            input_header, output_header,
            select_query,
            source_table_id, storages.at(source_table_id), metadata_snapshots.at(source_table_id),
            view_id, storages.at(view_id), metadata_snapshots.at(view_id),
            inner_table_id, inner_storage, metadata_snapshots.at(inner_table_id),
            select_context);

        executing_inner_query->setRuntimeData(thread_groups.at(view_id));

        result.addSource(std::move(executing_inner_query));
    }
    else
    {
        auto executing_inner_query = std::make_shared<ExecutingInnerQueryFromViewTransform<PullingPipelineExecutor>>(
            input_header, output_header,
            select_query,
            source_table_id, storages.at(source_table_id), metadata_snapshots.at(source_table_id),
            view_id, storages.at(view_id), metadata_snapshots.at(view_id),
            inner_table_id, inner_storage, metadata_snapshots.at(inner_table_id),
            select_context);

        executing_inner_query->setRuntimeData(thread_groups.at(view_id));

        result.addSource(std::move(executing_inner_query));
    }

    return result;
}


Chain InsertDependenciesBuilder::createPreSink(StorageIDMaybeEmpty view_id) const
{
    chassert(!skip_destination_table);
    chassert(view_id == root_view);

    Chain result;

    auto inner_table_id = inner_tables.at(view_id);
    auto inner_metadata = metadata_snapshots.at(inner_table_id);
    auto output_header = output_headers.at(view_id);
    auto insert_context = insert_contexts.at(view_id);

    auto adding_missing_defaults_dag = addMissingDefaults(
        *input_headers.at(view_id),
        output_header->getNamesAndTypesList(),
        inner_metadata->getColumns(),
        insert_context,
        insert_null_as_default);

    auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(
        *input_headers.at(view_id),
        adding_missing_defaults_dag.getRequiredColumnsNames(),
        insert_context);

    auto merged_dag = ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(adding_missing_defaults_dag));

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    result.addSink(std::make_shared<ConvertingTransform>(input_headers.at(view_id), std::make_shared<ExpressionActions>(std::move(merged_dag))));

    inner_metadata->check(result.getOutputHeader().getColumnsWithTypeAndName());

    return result;
}


Chain InsertDependenciesBuilder::createSink(StorageIDMaybeEmpty view_id) const
{
    const auto & inner_table_id = inner_tables.at(view_id);
    const auto & inner_storage = storages.at(inner_table_id);
    const auto & inner_metadata = metadata_snapshots.at(inner_table_id);
    const auto & insert_context = insert_contexts.at(view_id);
    const auto & header = output_headers.at(view_id);

    IInterpreter::checkStorageSupportsTransactionsIfNeeded(inner_storage, insert_context);

    Chain result;

    /// Add transform to remove Replicated columns. Right now no storage supports writing it.
    result.addSink(std::make_shared<RemovingReplicatedColumnsTransform>(header));

    /// Add transform to check if the sizes of arrays - elements of nested data structures doesn't match.
    /// We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
    /// NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
    /// but currently we don't have methods for serialization of nested structures "as a whole".
    result.addSink(std::make_shared<NestedElementsValidationTransform>(header));

    if (!inner_storage->supportsSparseSerialization())
        result.addSink(std::make_shared<RemovingSparseTransform>(header));

    auto constraints = buildConstraints(inner_metadata, inner_storage);
    if (!constraints.empty())
        result.addSink(std::make_shared<CheckConstraintsTransform>(inner_table_id, header, constraints, insert_context));

    if (auto * window_view = dynamic_cast<StorageWindowView *>(inner_storage.get()))
    {
        auto sink = std::make_shared<PushingToWindowViewSink>(std::make_shared<const Block>(window_view->getInputHeader()), *window_view, insert_context);
        sink->setRuntimeData(thread_groups.at(view_id));
        result.addSink(std::move(sink));
    }
    else if (dynamic_cast<StorageMaterializedView *>(inner_storage.get()))
    {
        // Data is never inserted to the StorageMaterializedView, it is inserted to its inner table
        UNREACHABLE();
    }
    else
    {
        auto sink = inner_storage->write(select_queries.at(view_id), metadata_snapshots.at(inner_table_id), insert_context, async_insert);
        sink->setRuntimeData(thread_groups.at(view_id));
        result.addSink(std::move(sink));
    }

    return result;
}


Chain InsertDependenciesBuilder::createPostSink(StorageIDMaybeEmpty view_id) const
{
    const auto & dependent_views_ids = dependent_views.at(view_id);
    if (dependent_views_ids.empty())
        return {};

    std::vector<Chain> view_chains;
    view_chains.reserve(dependent_views_ids.size());

    std::vector<Block> output_view_chains_headers;
    output_view_chains_headers.reserve(dependent_views_ids.size());

    for (const auto & child_view_id : dependent_views_ids)
    {
        ProfileEvents::increment(ProfileEvents::InsertQueriesWithSubqueries);
        ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);

        auto chain = Chain(std::make_shared<BeginingViewsTransform>(input_headers.at(child_view_id)));

        chain = Chain::concat(std::move(chain), createSelect(child_view_id));
        chain = Chain::concat(std::move(chain), createSink(child_view_id));
        chain = Chain::concat(std::move(chain), createPostSink(child_view_id));

        output_view_chains_headers.push_back(chain.getOutputHeader());
        view_chains.push_back(std::move(chain));
    }

    auto copying_data = std::make_shared<CopyTransform>(output_headers.at(view_id), dependent_views_ids.size());
    auto finalizing_views = std::make_shared<FinalizingViewsTransform>(std::move(output_view_chains_headers), dependent_views_ids, shared_from_this(), views_error_registry);
    auto out = copying_data->getOutputs().begin();
    auto in = finalizing_views->getInputs().begin();

    std::list<ProcessorPtr> processors;
    QueryPlanResourceHolder resources;
    for (auto & chain : view_chains)
    {
        resources.append(chain.detachResources());
        connect(*out, chain.getInputPort());
        connect(chain.getOutputPort(), *in);
        ++in;
        ++out;
        processors.splice(processors.end(), std::move(chain.getProcessors()));
    }
    view_chains.clear();

    processors.emplace_front(std::move(copying_data));
    processors.emplace_back(std::move(finalizing_views));

    auto result = Chain(std::move(processors));
    result.attachResources(std::move(resources));

    return result;
}


String getCleanQueryAst(const ASTPtr q, ContextPtr context)
{
    String res = q->formatWithSecretsOneLine();
    if (auto masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(res);

    res = res.substr(0, context->getSettingsRef()[Setting::log_queries_cut_to_length]);

    return res;
}


void InsertDependenciesBuilder::logQueryView(StorageID view_id, std::exception_ptr exception, bool before_start) const
{
    const auto & settings = init_context->getSettingsRef();
    if (!view_id || !settings[Setting::log_queries] || !settings[Setting::log_query_views])
        return;

    auto event_status = getQueryViewStatus(exception, before_start);
    if (event_status < settings[Setting::log_queries_min_type])
        return;

    if (!thread_groups.contains(view_id) || !view_types.contains(view_id) || !inner_tables.contains(view_id))
        return;

    const auto & thread_group = thread_groups.at(view_id);
    if (!thread_group)
        return;

    const auto & view_type = view_types.at(view_id);
    const auto & inner_table_id = inner_tables.at(view_id);

    UInt64 elapsed_ms = thread_group->getGroupElapsedMs();

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
    element.view_type = view_type;
    element.view_query = getCleanQueryAst(select_queries.at(view_id), select_contexts.at(view_id));
    element.view_target = inner_table_id.getFullTableName();

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
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void InsertDependenciesBuilder::DependencyPath::pushBack(StorageIDMaybeEmpty id)
{
    if (visited.contains(id))
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION, "Dependencies of the table {} are cyclic. Dependencies {} are pointing to {}.", path.front(), debugInfo(), id);

    path.push_back(id);
    visited.insert(id);
}


void InsertDependenciesBuilder::DependencyPath::popBack()
{
    visited.erase(path.back());
    path.pop_back();
}


String InsertDependenciesBuilder::DependencyPath::debugInfo() const
{
    return fmt::format("{}", fmt::join(path, " -> "));
}


QueryViewsLogElement::ViewStatus InsertDependenciesBuilder::getQueryViewStatus(std::exception_ptr exception, bool before_start)
{
    if (before_start)
        return QueryViewsLogElement::ViewStatus::EXCEPTION_BEFORE_START;

    if (exception)
        return QueryViewsLogElement::ViewStatus::EXCEPTION_WHILE_PROCESSING;

    return QueryViewsLogElement::ViewStatus::QUERY_FINISH;
}


bool InsertDependenciesBuilder::isViewsInvolved() const
{
    return isView(init_table_id) || !dependent_views.at(root_view).empty();
}


StorageIDMaybeEmpty InsertDependenciesBuilder::DependencyPath::parent(size_t inheritance) const
{
    if (path.size() > inheritance)
    {
        auto it = path.rbegin();
        std::advance(it, inheritance);
        return *it;
    }
    return StorageIDMaybeEmpty{};
}


bool InsertDependenciesBuilder::isView(StorageIDMaybeEmpty id) const
{
    return inner_tables.contains(id);
}


Chain InsertDependenciesBuilder::createRetry(const std::vector<StorageIDMaybeEmpty> & path, StorageIDMaybeEmpty start_from, const std::string & partition) const
{
    chassert(!path.empty());

    LOG_DEBUG(logger, "Creating retry chain for path {}, partition <{}> starting from {}", fmt::join(path, "/"), partition, start_from);

    Chain result;

    auto it = std::find(path.begin(), path.end(), start_from);
    chassert(it != path.end());

    if (!skip_destination_table && it == path.begin())
    {
        const auto & view_id = path.front();
        chassert(isView(view_id));

        result = Chain::concat(std::move(result), createPreSink(view_id));

        ++it;
    }
    else if (it == path.begin())
        ++it;

    for (; it != path.end(); ++it)
    {
        // build nodes only for views in path
        if (!isView(*it))
            continue;

        const auto & view_id = *it;
        chassert(isView(view_id));

        result = Chain::concat(std::move(result), createSelect(view_id));
    }

    const auto & view_id = path.back();

    /// Add transform to remove Replicated columns. Right now no storage supports writing it.
    result.addSink(std::make_shared<RemovingReplicatedColumnsTransform>(output_headers.at(view_id)));

    result.addSink(
        std::make_shared<SelectPartitionTransform>(
        partition,
        metadata_snapshots.at(inner_tables.at(view_id)),
        insert_contexts.at(view_id),
        output_headers.at(view_id)));

    return result;
}


Chain InsertDependenciesBuilder::createChainForDeduplicationRetry(const DeduplicationInfo & info, const std::string & partition_id) const
{
    return createRetry(info.visited_views, info.original_block_view_id, partition_id);
}

}
