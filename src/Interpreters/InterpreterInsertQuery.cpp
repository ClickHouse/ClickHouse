#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Common/MemoryTrackerUtils.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <Common/MemoryTracker.h>
#include <Core/SettingsEnums.h>
#include <Core/ServerSettings.h>
#include <Core/DeduplicateInsert.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/processColumnTransformers.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/Transforms/ApplySquashingTransform.h>
#include <Processors/Transforms/ShrinkColumnsTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <Common/quoteString.h>
#include <Core/Field.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Processors/Sources/RemoteSource.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/JoinedTables.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <memory>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool distributed_foreground_insert;
    extern const SettingsBool insert_null_as_default;
    extern const SettingsBool optimize_trivial_insert_select;
    extern const SettingsBool parallel_view_processing;
    extern const SettingsDeduplicateInsertSelectMode deduplicate_insert_select;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsMaxThreads max_insert_threads;
    extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
    extern const SettingsUInt64 max_insert_threads_min_free_memory_per_thread;
    extern const SettingsBool use_strict_insert_block_limits;
    extern const SettingsUInt64Auto insert_quorum;
    extern const SettingsBool insert_quorum_parallel;
    extern const SettingsBool deduplicate_blocks_in_dependent_materialized_views;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsFloat shrink_over_allocated_columns_min_waste_ratio;
    extern const SettingsUInt64 shrink_over_allocated_columns_min_waste_bytes;
    extern const SettingsString insert_deduplication_token;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 parallel_distributed_insert_select;
    extern const SettingsBool enable_parsing_to_custom_serialization;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsBool parallel_replicas_insert_select_local_pipeline;
    extern const SettingsBool parallel_replicas_prefer_local_replica;
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsUInt64 max_distributed_depth;
    extern const SettingsBool enable_global_with_statement;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool add_implicit_sign_column_constraint_for_collapsing_engine;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int QUERY_IS_PROHIBITED;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int LOGICAL_ERROR;
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextMutablePtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_, bool async_insert_)
    : WithMutableContext(context_)
    , logger(getLogger("InterpreterInsertQuery"))
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
    , async_insert(async_insert_)
{
    checkStackSize();
    if (auto quota = getContext()->getQuota())
        quota->checkExceededForQuery(getContext()->getNormalizedQueryHash(), QuotaType::WRITTEN_BYTES);

    const Settings & settings = getContext()->getSettingsRef();
    max_threads = getMaxThreadsForAvailableMemory(
        std::max<size_t>(1, settings[Setting::max_threads]),
        settings[Setting::max_threads_min_free_memory_per_thread]);
    max_insert_threads = getMaxThreadsForAvailableMemory(
        std::min(std::max<size_t>(1, settings[Setting::max_insert_threads]), max_threads),
        settings[Setting::max_insert_threads_min_free_memory_per_thread]);
}

StoragePtr InterpreterInsertQuery::getTable(ASTInsertQuery & query)
{
    auto current_context = getContext();

    if (query.table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(query.table_function, current_context);

        /// If table function needs structure hint from select query
        /// we can create a temporary pipeline and get the header.
        if (query.select && table_function_ptr->needStructureHint())
        {
            SharedHeader header_block;
            auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

            if (current_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                header_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query.select, current_context, select_query_options);
            }
            else
            {
                ASTPtr input_function;
                query.tryFindInputFunction(input_function);
                if (input_function)
                    throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Schema inference is not supported with allow_experimental_analyzer=0 for INSERT INTO FUNCTION ... SELECT FROM input()");

                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, current_context, select_query_options};
                auto tmp_pipeline = interpreter_select.buildQueryPipeline();
                header_block = tmp_pipeline.getSharedHeader();
            }

            ColumnsDescription structure_hint{header_block->getNamesAndTypesList()};
            table_function_ptr->setStructureHint(structure_hint);
        }

        table_function_ptr->setPartitionBy(query.partition_by);

        return table_function_ptr->execute(query.table_function, current_context, table_function_ptr->getName(),
                                           /* cached_columns */ {}, /* use_global_context */ false, /* is_insert_query */true);
    }

    if (query.table_id)
    {
        query.table_id = current_context->resolveStorageID(query.table_id);
    }
    else
    {
        /// Insert query parser does not fill table_id because table and
        /// database can be parameters and be filled after parsing.
        StorageID local_table_id(query.getDatabase(), query.getTable());
        query.table_id = current_context->resolveStorageID(local_table_id);
    }

    return DatabaseCatalog::instance().getTable(query.table_id, current_context);
}

Block InterpreterInsertQuery::getSampleBlock(
    const ASTInsertQuery & query,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_,
    bool no_destination,
    bool allow_materialized)
{
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);
        return metadata_snapshot->getSampleBlockNonMaterialized();
    }

    /// Form the block based on the column names from the query
    const auto columns_ast = processColumnTransformers(context_->getCurrentDatabase(), table, metadata_snapshot, query.columns);
    Names names;
    names.reserve(columns_ast->children.size());
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();
        names.emplace_back(std::move(current_name));
    }

    return getSampleBlock(names, table, metadata_snapshot, no_destination, allow_materialized);
}

Block InterpreterInsertQuery::getSampleBlock(
    const Names & names,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    bool allow_virtuals,
    bool allow_materialized)
{
    std::vector<size_t> missing_positions;
    Block table_sample_insertable = metadata_snapshot->getSampleBlockInsertable();

    ColumnsWithTypeAndName res{names.size()};
    std::unordered_set<String> inserted_names;

    for (size_t i = 0; i < names.size(); i++)
    {
        const auto & current_name = names[i];
        if (!inserted_names.insert(current_name).second)
            throw Exception(
                ErrorCodes::DUPLICATE_COLUMN,
                "Column {} in table {} specified more than once",
                current_name,
                table->getStorageID().getNameForLogs());

        const ColumnWithTypeAndName * insertable_col = table_sample_insertable.findByName(current_name);
        if (!insertable_col)
            missing_positions.emplace_back(i);
        else
            res[i] = *insertable_col;
    }

    if (!missing_positions.empty())
    {
        Block table_sample_physical = metadata_snapshot->getSampleBlock();
        Block table_sample_virtuals;
        if (allow_virtuals)
            table_sample_virtuals = metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::All);

        /// Columns are not ordinary or ephemeral
        for (auto pos : missing_positions)
        {
            const auto & current_name = names[pos];

            if (table_sample_physical.has(current_name))
            {
                /// Column is materialized
                if (!allow_materialized)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert column {}, because it is MATERIALIZED column", current_name);
                res[pos] = table_sample_physical.getByName(current_name);
            }
            else if (table_sample_virtuals.has(current_name))
            {
                res[pos] = table_sample_virtuals.getByName(current_name);
            }
            else
            {
                /// The table does not have a column with that name
                throw Exception(
                    ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                    "No such column {} in table {}",
                    current_name,
                    table->getStorageID().getNameForLogs());
            }
        }
    }

    return res;
}

static bool hasAggregateFunctions(const IAST * ast)
{
    if (const auto * func = typeid_cast<const ASTFunction *>(ast))
        if (AggregateUtils::isAggregateFunction(*func))
            return true;

    for (const auto & child : ast->children)
        if (hasAggregateFunctions(child.get()))
            return true;

    return false;
}
/** A query that just reads all data without any complex computations or filetering.
  * If we just pipe the result to INSERT, we don't have to use too many threads for read.
  */
static bool isTrivialSelect(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// Note: how to write it in more generic way?
        return (!select_query->distinct
            && !select_query->limit_with_ties
            && !select_query->prewhere()
            && !select_query->where()
            && !select_query->groupBy()
            && !select_query->having()
            && !select_query->orderBy()
            && !select_query->limitBy()
            && !hasAggregateFunctions(select_query));
    }
    /// This query is ASTSelectWithUnionQuery subquery
    return false;
}

bool InterpreterInsertQuery::shouldAddSquashingForStorage(const StoragePtr & table, ContextPtr context_)
{
    const Settings & settings = context_->getSettingsRef();

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    return !(settings[Setting::distributed_foreground_insert] && table->isRemote());
}

static std::pair<QueryPipelineBuilder, ClusterProxy::LocalPlanParallelReplicasInfo> getLocalSelectPipelineForInserSelectWithParallelReplicas(const ASTPtr & select, const ContextPtr & context)
{
    auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, /*subquery_depth_=*/1);

    InterpreterSelectQueryAnalyzer interpreter(select, context, select_query_options);
    auto & plan = interpreter.getQueryPlan();

    /// Find reading steps for remote replicas and remove them,
    /// When building local pipeline, the local replica will be registered in the returned coordinator,
    /// and announce its snapshot. The snapshot will be used to assign read tasks to involved replicas
    /// So, the remote pipelines, which will be created later, should use the same coordinator.
    /// The connection pools and local replica index decided here are returned too, so the remote pass
    /// reuses the exact same replica set rather than recomputing liveness from a fresh snapshot.
    auto parallel_replicas_info = ClusterProxy::dropReadFromRemoteInPlan(plan);
    return {interpreter.buildQueryPipeline(), std::move(parallel_replicas_info)};
}


QueryPipeline InterpreterInsertQuery::addInsertToSelectPipeline(ASTInsertQuery & query, StoragePtr table, QueryPipelineBuilder & pipeline)
{
    auto context = getContext();

    // disable parallel replicas for inserts if enabled
    // the insert can trigger update for dependent materialized views
    // using parallel replicas in this context is unnecessary
    if (context->canUseParallelReplicasOnInitiator())
    {
        auto mutable_context = Context::createCopy(context);
        mutable_context->setSetting("enable_parallel_replicas", Field{0});
        context = mutable_context;
    }

    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, context, no_destination, allow_materialized);

    pipeline.dropTotalsAndExtremes();

    /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
    if (context->getSettingsRef()[Setting::insert_null_as_default])
    {
        const auto & input_columns = pipeline.getHeader().getColumnsWithTypeAndName();
        const auto & query_columns = query_sample_block.getColumnsWithTypeAndName();
        const auto & output_columns = metadata_snapshot->getColumns();

        if (input_columns.size() == query_columns.size())
        {
            for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
            {
                /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                /// default column values (in AddingDefaultsTransform), so all values will be cast correctly.
                if (isNullableOrLowCardinalityNullable(input_columns[col_idx].type)
                    && !isNullableOrLowCardinalityNullable(query_columns[col_idx].type)
                    && !isVariant(query_columns[col_idx].type)
                    && !isDynamic(query_columns[col_idx].type)
                    && output_columns.has(query_columns[col_idx].name))
                {
                    query_sample_block.setColumn(
                        col_idx,
                        ColumnWithTypeAndName(
                            makeNullableOrLowCardinalityNullable(query_columns[col_idx].column),
                            makeNullableOrLowCardinalityNullable(query_columns[col_idx].type),
                            query_columns[col_idx].name));
                }
            }
        }
    }

    auto actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            query_sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            context);
    auto actions = std::make_shared<ExpressionActions>(std::move(actions_dag), ExpressionActionsSettings(context, CompileExpressions::yes));

    pipeline.addSimpleTransform([&](const SharedHeader & in_header) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(in_header, actions);
    });

    pipeline.addSimpleTransform([&](const SharedHeader & in_header) -> ProcessorPtr
    {
        auto counting = std::make_shared<CountingTransform>(in_header, context->getQuota(), context->getNormalizedQueryHash());
        counting->setProcessListElement(context->getProcessListElement());
        counting->setProgressCallback(context->getProgressCallback());

        return counting;
    });

    auto select_streams = pipeline.getNumStreams();
    if (select_streams != 1)
        pipeline.resize(1);

    auto deduplicate_insert_select = isDeduplicationEnabledForInsertSelect(
        select_query_sorted, context->getSettingsRef(),
        context->getSettingsRef()[Setting::insert_deduplication_token].value, logger);

    if (deduplicate_insert_select != isDeduplicationEnabledForInsert(false, context->getSettingsRef()))
    {
        auto tmp_context = Context::createCopy(context);
        overrideDeduplicationSetting(deduplicate_insert_select, tmp_context);
        context = tmp_context;
    }

    auto insert_dependencies = InsertDependenciesBuilder::create(
        table, query_ptr, std::make_shared<const Block>(std::move(query_sample_block)),
        async_insert, /*skip_destination_table*/ no_destination, max_insert_threads,
        context);

    const auto & settings = context->getSettingsRef();
    bool squash_with_strict_limits = settings[Setting::use_strict_insert_block_limits] && !async_insert;

    if (!squash_with_strict_limits)
    {
        pipeline.addSimpleTransform([&](const SharedHeader & in_header) -> ProcessorPtr
        {
            return std::make_shared<AddDeduplicationInfoTransform>(
                insert_dependencies,
                insert_dependencies->getRootViewID(),
                context->getSettingsRef()[Setting::insert_deduplication_token].value,
                in_header);
        });
    }

    bool should_squash = shouldAddSquashingForStorage(table, getContext()) && !no_squash;
    if (should_squash)
    {
        pipeline.addSimpleTransform(
            [&](const SharedHeader & in_header) -> ProcessorPtr
            {
                size_t min_block_size_bytes = table->prefersLargeBlocks() ? context->getSettingsRef()[Setting::min_insert_block_size_bytes] : 0ULL;
                /// On low-memory systems, cap squashing block size to avoid accumulating too much data.
                if (auto memory_limit = total_memory_tracker.getHardLimit(); memory_limit > 0)
                    min_block_size_bytes = std::min<size_t>(min_block_size_bytes, static_cast<size_t>(static_cast<double>(memory_limit) * 0.9) / 8);
                return std::make_shared<PlanSquashingTransform>(
                    in_header,
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                    min_block_size_bytes,
                    settings[Setting::max_insert_block_size],
                    settings[Setting::max_insert_block_size_bytes],
                    squash_with_strict_limits);
            });
    }

    VectorWithMemoryTracking<Chain> sink_chains = insert_dependencies->createChainWithDependenciesForAllStreams();

    pipeline.resize(insert_dependencies->getSinkStreamSize());

    if (should_squash)
    {
        pipeline.addSimpleTransform(
            [&](const SharedHeader & in_header) -> ProcessorPtr
            {
                return std::make_shared<ApplySquashingTransform>(in_header);
            });
    }

    if (squash_with_strict_limits)
    {
        pipeline.addSimpleTransform([&](const SharedHeader & in_header) -> ProcessorPtr
        {
            return std::make_shared<AddDeduplicationInfoTransform>(
                insert_dependencies,
                insert_dependencies->getRootViewID(),
                settings[Setting::insert_deduplication_token].value,
                in_header);
        });
    }

    for (auto & chain : sink_chains)
    {
        pipeline.addResources(chain.detachResources());
    }
    pipeline.addChains(std::move(sink_chains));

    pipeline.setMaxThreads(max_threads);
    // Cap to 1 when parallel_view_processing=0. Pipe::max_parallel_streams is a watermark that
    // resize() does not lower, so limitMaxThreads is needed even after resize(sink_stream_size).
    pipeline.limitMaxThreads(insert_dependencies->getViewProcessingNumThreads());

    pipeline.setSinks([&](const SharedHeader & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<EmptySink>(cur_header);
    });

    return QueryPipelineBuilder::getPipeline(std::move(pipeline));
}

static void applyTrivialInsertSelectOptimization(ASTInsertQuery & query, bool prefer_large_blocks, size_t effective_max_insert_threads, ContextPtr & select_context)
{
    const Settings & settings = select_context->getSettingsRef();

    bool is_trivial_insert_select = false;

    if (settings[Setting::optimize_trivial_insert_select])
    {
        const auto & select_query = query.select->as<ASTSelectWithUnionQuery &>();
        const auto & selects = select_query.list_of_selects->children;
        const auto & union_modes = select_query.list_of_modes;

        /// ASTSelectWithUnionQuery is not normalized now, so it may pass some queries which can be Trivial select queries
        const auto mode_is_all = [](const auto & mode) { return mode == SelectUnionMode::UNION_ALL; };

        is_trivial_insert_select =
            std::all_of(union_modes.begin(), union_modes.end(), std::move(mode_is_all))
            && std::all_of(selects.begin(), selects.end(), isTrivialSelect);
    }

    if (is_trivial_insert_select)
    {
        /** When doing trivial INSERT INTO ... SELECT ... FROM table,
            * don't need to process SELECT with more than max_insert_threads
            * and it's reasonable to set block size for SELECT to the desired block size for INSERT
            * to avoid unnecessary squashing.
            */

        Settings new_settings = select_context->getSettingsCopy();

        /// Use the effective value computed in the constructor: it is already capped by `max_threads`
        /// and reduced according to the available memory, while the raw setting is not.
        new_settings[Setting::max_threads] = effective_max_insert_threads;

        if (prefer_large_blocks)
        {
            if (settings[Setting::min_insert_block_size_rows])
                new_settings[Setting::max_block_size] = settings[Setting::min_insert_block_size_rows];
            if (settings[Setting::min_insert_block_size_bytes])
            {
                size_t block_size_bytes = settings[Setting::min_insert_block_size_bytes];
                /// On low-memory systems, cap the input format block size.
                if (auto memory_limit = total_memory_tracker.getHardLimit(); memory_limit > 0)
                    block_size_bytes = std::min<size_t>(block_size_bytes, static_cast<size_t>(static_cast<double>(memory_limit) * 0.9) / 8);
                new_settings[Setting::preferred_block_size_bytes] = block_size_bytes;
            }
        }

        auto context_for_trivial_select = Context::createCopy(select_context);
        context_for_trivial_select->setSettings(new_settings);

        select_context = context_for_trivial_select;
    }
}

static bool queryHasOrderByAll(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        return select_query->order_by_all;
    }
    else if (auto * union_query = select->as<ASTSelectWithUnionQuery>())
    {
        if (union_query->list_of_selects->children.size() != 1)
            return false;

        if (auto * first_select_query = union_query->list_of_selects->children.front()->as<ASTSelectQuery>())
            return first_select_query->order_by_all;
    }
    return false;
}

QueryPipeline InterpreterInsertQuery::buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table)
{
    ContextPtr select_context = getContext();
    applyTrivialInsertSelectOptimization(query, table->prefersLargeBlocks(), max_insert_threads, select_context);

    QueryPipelineBuilder pipeline = [&]()
    {
        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

        const Settings & settings = select_context->getSettingsRef();
        if (settings[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter_select_analyzer(query.select, select_context, select_query_options);
            return interpreter_select_analyzer.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectWithUnionQuery interpreter_select(query.select, select_context, select_query_options);
            return interpreter_select.buildQueryPipeline();
        }
    }();

    /// ORDER BY ALL should produce a single globally-sorted stream.
    /// However, certain edge cases (e.g., BuzzHouse fuzzer findings on specific
    /// table engines or optimizer paths) can result in multiple streams.
    /// Treat multi-stream output as unsorted — deduplication won't be enabled,
    /// but the query executes correctly since addInsertToSelectPipeline()
    /// resizes to 1 stream regardless.
    select_query_sorted = queryHasOrderByAll(query.select) && pipeline.getNumStreams() <= 1;

    return addInsertToSelectPipeline(query, table, pipeline);
}


std::pair<QueryPipeline, ClusterProxy::LocalPlanParallelReplicasInfo> InterpreterInsertQuery::buildLocalInsertSelectPipelineForParallelReplicas(
    ASTInsertQuery & query, const StoragePtr & table, ContextPtr select_context)
{
    applyTrivialInsertSelectOptimization(query, table->prefersLargeBlocks(), max_insert_threads, select_context);

    auto [pipeline_builder, parallel_replicas_info]
        = getLocalSelectPipelineForInserSelectWithParallelReplicas(query.select, select_context);
    auto local_pipeline = addInsertToSelectPipeline(query, table, pipeline_builder);
    return {std::move(local_pipeline), std::move(parallel_replicas_info)};
}


static bool isInsertSelectTrivialEnoughForDistributedExecution(const ASTInsertQuery & query)
{
    const auto & select = query.select->as<ASTSelectWithUnionQuery &>();
    const auto & selects = select.list_of_selects->children;
    if (selects.size() != 1)
        return {};

    if (auto * select_query = selects.front()->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// TODO: replace with QueryTree analysis after switching to analyzer completely
        return (!select_query->distinct
            && !select_query->limit_with_ties
            && !select_query->groupBy()
            && !select_query->having()
            && !select_query->orderBy()
            && !select_query->limitBy()
            && !select_query->limitLength()
            && !hasAggregateFunctions(select_query));
    }
    return false;
}


std::optional<QueryPipeline> InterpreterInsertQuery::buildInsertSelectPipelineParallelReplicas(ASTInsertQuery & query, StoragePtr table)
{
    const Settings & settings = getContext()->getSettingsRef();
    if (!settings[Setting::allow_experimental_analyzer])
        return {};

    if (settings[Setting::parallel_distributed_insert_select] != 2)
        return {};

    /// Create a context with automatic_parallel_replicas_mode disabled upfront.
    /// INSERT SELECT should use parallel replicas regardless of automatic mode,
    /// and followers need automatic_parallel_replicas_mode == 0 to participate in coordinated reading.
    auto context = Context::createCopy(getContext());
    context->setSetting("automatic_parallel_replicas_mode", Field{0});

    if (!context->canUseParallelReplicasOnInitiator())
        return {};

    // NOTE: should we limit it more here?
    if (auto storage = getTable(query); storage->isMergeTree() && !storage->supportsReplication())
        return {};

    if (!isInsertSelectTrivialEnoughForDistributedExecution(query))
        return {};

    auto select = query.select->as<ASTSelectWithUnionQuery &>().list_of_selects->children.front();
    if (!ClusterProxy::isSuitableForInsertSelectWithParallelReplicas(select, context))
        return {};

    LOG_TRACE(logger, "Building distributed insert select pipeline with parallel replicas: table={}", query.getTable());

    if (settings[Setting::parallel_replicas_local_plan] && settings[Setting::parallel_replicas_insert_select_local_pipeline]
        && settings[Setting::parallel_replicas_prefer_local_replica])
    {
        auto [local_pipeline, parallel_replicas_info] = buildLocalInsertSelectPipelineForParallelReplicas(query, table, context);
        auto coordinator = parallel_replicas_info.coordinator;
        auto local_replica_index = parallel_replicas_info.local_replica_index;
        return ClusterProxy::executeInsertSelectWithParallelReplicas(
            query,
            context,
            std::move(local_pipeline),
            std::move(coordinator),
            std::move(parallel_replicas_info.connection_pools),
            local_replica_index);
    }

    return ClusterProxy::executeInsertSelectWithParallelReplicas(query, context);
}


QueryPipeline InterpreterInsertQuery::buildInsertPipeline(ASTInsertQuery & query, StoragePtr table)
{
    auto context = getContext();

    // disable parallel replicas for inserts if enabled
    // the insert can trigger update for dependent materialized views
    // using parallel replicas in this context is unnecessary
    if (context->canUseParallelReplicasOnInitiator())
    {
        auto mutable_context = Context::createCopy(context);
        mutable_context->setSetting("enable_parallel_replicas", Field{0});
        context = mutable_context;
    }

    const Settings & settings = context->getSettingsRef();
    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
    auto query_sample_block
        = std::make_shared<const Block>(getSampleBlock(query, table, metadata_snapshot, context, no_destination, allow_materialized));
    if (query_sample_block->empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Empty list of columns to insert");

    // when insert is initiated from FileLog or similar storages
    // they are allowed to expose its virtuals columns to the dependent views
    //
    // Pass `max_insert_threads` so that the writing side of a plain INSERT (data coming from
    // clickhouse-client or over the HTTP interface, not from a SELECT) can be parallelized too.
    // The input is always a single stream; we resize the pipeline to `sink_stream_size` parallel
    // streams after the data is read and the squashing is planned. `InsertDependenciesBuilder`
    // keeps `sink_stream_size` at 1 (preserving the previous behavior) unless all destinations
    // support parallel inserts, so this stays a no-op for the default `max_insert_threads = 0`.
    // Asynchronous inserts have their own batching/flush mechanism, so they keep a single stream.
    //
    // With `use_strict_insert_block_limits`, the deduplication info (source block number) is stamped
    // by a per-stream `AddDeduplicationInfoTransform` *after* the fan-out (see below), so each parallel
    // branch restarts its block numbering from zero. The unified deduplication id folds in that source
    // block number for any synchronous insert - both for a non-empty `insert_deduplication_token` (the
    // id is `token` + source block number, independent of the block contents) and for a token-less
    // insert (the id is the data hash + source block number). Two identical squashed blocks that land
    // on different branches therefore get identical ids, which `MergeTreeSink` /
    // `ReplicatedMergeTreeSink` treat as duplicates and skip - silently dropping rows of a single
    // parallel `INSERT`. Keep such strict inserts single-stream (as before), so the numbering stays
    // global.
    //
    // The same collision arises without strict limits when the destination storage forwards the data
    // through a nested `INSERT` that stamps the deduplication info from scratch (`Distributed`,
    // `Buffer`): each parallel branch gets its own sink, whose nested `INSERT` restarts the source
    // block numbering per branch even though this query stamped it globally in the single-stream head
    // of the pipeline. An `Alias` is different: its `AliasSink` runs the nested `INSERT` in this
    // query's context with the chunk's deduplication info intact, and an already-stamped chunk is not
    // restamped, so the globally stamped numbering survives the hop and the fan-out stays safe
    // without strict limits - an `Alias` behaves like the table it forwards to.
    //
    // This only matters when the destination sink actually deduplicates: the colliding id is consulted
    // only by a MergeTree-family table with its deduplication window enabled, and only when deduplication
    // is not disabled by `deduplicate_insert` / `insert_deduplicate`. For a table that never deduplicates
    // (e.g. a `MergeTree` with `non_replicated_deduplication_window = 0`, a `Memory`/`Null` table, or a
    // session with deduplication disabled) the collision is harmless, so the fan-out stays safe and
    // `max_insert_threads` keeps applying.
    //
    // The analogous VIEW-level collision for dependent materialized views (a per-branch source block
    // number folded into the view-level ids under strict limits, or a dependent target that forwards
    // the write through a nested `INSERT`) is handled inside `InsertDependenciesBuilder`, which keeps
    // its sink stream size at 1 in that case regardless of the value passed here.
    const bool dedup_enabled_for_insert = isDeduplicationEnabledForInsert(async_insert, settings);
    const bool source_deduplicates = InsertDependenciesBuilder::storageDeduplicatesBlocksOnInsert(table)
        && dedup_enabled_for_insert;
    const bool rebuilds_dedup_ids = InsertDependenciesBuilder::storageRebuildsDeduplicationIdsOnInsert(table);
    const bool per_branch_dedup_ids = settings[Setting::use_strict_insert_block_limits]
        || rebuilds_dedup_ids;

    // A forwarding storage (`Alias`, `Distributed`, `Buffer`) runs a nested `INSERT` per sink branch.
    // That nested `INSERT` can reach a deduplicating dependent materialized view even when the
    // forwarded-to table itself never deduplicates (e.g. an `Alias` over a `MergeTree` with
    // `non_replicated_deduplication_window = 0` whose materialized view targets a deduplicating table).
    // The dependent-MV chain of the forwarded-to table lives behind the nested `INSERT` and is not
    // visible to this pipeline (`InsertDependenciesBuilder` only expands the dependencies of the
    // immediate target), so it must be guarded here. The view-level deduplication ids fold in the
    // source block number, so they stay distinct across branches as long as the source numbering is
    // global. Fail closed when the numbering is per-branch and the nested `INSERT` can reach a
    // dependent view: either the forwarding chain restarts the numbering on its own (`Distributed` /
    // `Buffer` - also kept single-stream by `forwards_to_separate_context` below), or
    // `use_strict_insert_block_limits` stamps it per branch after the fan-out and the per-branch
    // numbers survive the hop into the dependent-view graph hidden behind an `Alias`.
    const bool forwarded_dependent_mv_dedup_hazard = dedup_enabled_for_insert
        && settings[Setting::deduplicate_blocks_in_dependent_materialized_views]
        && ((rebuilds_dedup_ids && InsertDependenciesBuilder::forwardedInsertReachesDependentView(table))
            || (settings[Setting::use_strict_insert_block_limits]
                && InsertDependenciesBuilder::forwardedInsertHidesDependentView(table)));

    // A `Buffer` flushes its accumulated data to the destination through a nested `INSERT` built from the
    // buffer's *own* context (`StorageBuffer::writeBlockToDestination` copies `getContext()`, not this
    // query's context), and a `Distributed` forwards the write to a remote shard whose table is not cheaply
    // known here and may itself be (or forward to) such a `Buffer`. In both cases this query's
    // `deduplicate_insert` / `insert_deduplicate` / `deduplicate_blocks_in_dependent_materialized_views`
    // settings do not govern the final write. Disabling deduplication for this `INSERT` therefore does not
    // make the write fan-out safe: the downstream flush can still deduplicate on its destination while each
    // parallel branch restarts the source block numbering from zero, so identical blocks on different
    // branches collide and rows are silently dropped. Fail closed and keep such inserts single-stream
    // regardless of the deduplication settings on this query. (Unlike an `Alias`, whose `AliasSink` runs its
    // nested `INSERT` in this query's context and so does observe a `deduplicate_insert = disable` here.)
    const bool forwards_to_separate_context =
        InsertDependenciesBuilder::storageForwardsInsertToSeparateContext(table);

    /// An `Alias` itself keeps the nested `INSERT` in this query's context, but the dependent-view
    /// graph of its target - hidden behind the nested `INSERT` each `AliasSink` runs - can contain a
    /// materialized view whose target is a `Buffer` or a `Distributed`. That hidden separate-context
    /// sink drops the carried deduplication info (`BufferSink` / `DistributedSink` restamp the source
    /// block numbering from scratch in another context), so with a fan-out to several `AliasSink`s
    /// identical blocks from different branches can still collide on the final deduplicating
    /// destination - even when this query disabled deduplication, because those settings never reach
    /// the separate-context write. The visible variant of this topology is failed closed inside
    /// `InsertDependenciesBuilder`; the hidden-behind-an-`Alias` variant must be failed closed here,
    /// independent of the deduplication settings on this query.
    const bool hidden_views_forward_to_separate_context =
        InsertDependenciesBuilder::forwardedInsertHidesDependentViewForwardingToSeparateContext(table, context);

    // `parallel_view_processing = 0` keeps the pushing to dependent materialized views sequential.
    // For a dependent-view graph visible to `InsertDependenciesBuilder` this is enforced there (the
    // sink stream size stays 1 when views are involved and the setting is disabled) and by the
    // single-thread pipeline cap below. A forwarding storage hides its target's dependent-view
    // graph behind the nested `INSERT` its sink runs per branch (`AliasSink`), so a fan-out to
    // several sinks would push those hidden views concurrently even though
    // `parallel_view_processing` is disabled. Keep such inserts single-stream, independently of any
    // deduplication hazard. (`Distributed` and `Buffer` also hide their dependent views, but they
    // are already kept single-stream by `forwards_to_separate_context`.)
    const bool serial_hidden_views = !settings[Setting::parallel_view_processing]
        && InsertDependenciesBuilder::forwardedInsertHidesDependentView(table);

    const bool dedup_single_stream = !async_insert
        && ((per_branch_dedup_ids && source_deduplicates)
            || forwarded_dependent_mv_dedup_hazard
            || forwards_to_separate_context
            || hidden_views_forward_to_separate_context);

    /// A non-parallel quorum insert (`insert_quorum >= 2` or `'auto'`, with `insert_quorum_parallel = 0`)
    /// permits a single in-flight quorum part per table: every `ReplicatedMergeTreeSink` checks in
    /// `onStart` that the quorum of all previous writes is already satisfied (`checkQuorumPrecondition`)
    /// and throws `UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE` otherwise. With a write fan-out every branch
    /// runs its own sink - including branches that receive no data - so sibling sinks of the same
    /// `INSERT` race against the not-yet-satisfied quorum node of the part committed by the branch that
    /// got the data. Keep such inserts single-stream.
    const bool sequential_quorum_insert = !settings[Setting::insert_quorum_parallel]
        && (settings[Setting::insert_quorum].is_auto || settings[Setting::insert_quorum].valueOr(0) >= 2);

    const size_t insert_threads
        = (async_insert || dedup_single_stream || serial_hidden_views || sequential_quorum_insert) ? 1 : max_insert_threads;
    auto insert_dependencies = InsertDependenciesBuilder::create(
        table,
        query_ptr,
        query_sample_block,
        async_insert,
        /*skip_destination_table*/ no_destination,
        insert_threads,
        context);

    auto sink_chains = insert_dependencies->createChainWithDependenciesForAllStreams();
    const size_t sink_stream_size = insert_dependencies->getSinkStreamSize();
    chassert(sink_chains.size() == sink_stream_size);
    chassert(sink_stream_size >= 1);

    bool squash_with_strict_limits = settings[Setting::use_strict_insert_block_limits] && !async_insert;
    bool should_squash = shouldAddSquashingForStorage(table, context) && !no_squash;

    /// The header that flows through the whole insert pipeline.
    SharedHeader insert_header = sink_chains.front().getInputSharedHeader();

    auto processors = std::make_shared<Processors>();

    /// Build the single-stream head of the pipeline. It processes the input data
    /// (counting, deduplication info, planning of squashing) before the data is
    /// distributed across the parallel insert streams.
    InputPort * pipeline_input = nullptr;
    OutputPort * head_output = nullptr;

    auto add_head_transform = [&](ProcessorPtr processor)
    {
        chassert(processor->getInputs().size() == 1);
        chassert(processor->getOutputs().size() == 1);
        if (head_output)
            connect(*head_output, processor->getInputs().front());
        else
            pipeline_input = &processor->getInputs().front();
        head_output = &processor->getOutputs().front();
        processors->emplace_back(std::move(processor));
    };

    /// Shrink over-allocated columns produced by parsing (e.g. String columns grown power-of-two) to
    /// fit, right after the source where the chunk is uniquely owned, to reduce peak memory usage.
    if (static_cast<double>(settings[Setting::shrink_over_allocated_columns_min_waste_ratio]) > 1.0)
        add_head_transform(std::make_shared<ShrinkColumnsTransform>(
            insert_header,
            static_cast<double>(settings[Setting::shrink_over_allocated_columns_min_waste_ratio]),
            settings[Setting::shrink_over_allocated_columns_min_waste_bytes]));

    {
        auto counting = std::make_shared<CountingTransform>(insert_header, context->getQuota(), context->getNormalizedQueryHash());
        counting->setProcessListElement(context->getProcessListElement());
        counting->setProgressCallback(context->getProgressCallback());
        add_head_transform(std::move(counting));
    }

    if (!squash_with_strict_limits)
        add_head_transform(std::make_shared<AddDeduplicationInfoTransform>(
            insert_dependencies,
            insert_dependencies->getRootViewID(),
            settings[Setting::insert_deduplication_token].value,
            insert_header));

    if (should_squash)
    {
        bool table_prefers_large_blocks = table->prefersLargeBlocks();
        size_t min_block_size_bytes = table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL;
        /// On low-memory systems, cap squashing block size to avoid accumulating too much data.
        if (auto memory_limit = total_memory_tracker.getHardLimit(); memory_limit > 0)
            min_block_size_bytes = std::min<size_t>(min_block_size_bytes, static_cast<size_t>(static_cast<double>(memory_limit) * 0.9) / 8);
        add_head_transform(std::make_shared<PlanSquashingTransform>(
            insert_header,
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
            min_block_size_bytes,
            settings[Setting::max_insert_block_size],
            settings[Setting::max_insert_block_size_bytes],
            squash_with_strict_limits));
    }

    /// Prepend the per-stream transforms to each sink chain. `addSource` prepends, so the
    /// resulting top-to-bottom order matches the previous single-stream pipeline:
    /// ApplySquashing -> AddDeduplicationInfo (strict) -> sink.
    for (auto & sink_chain : sink_chains)
    {
        if (squash_with_strict_limits)
            sink_chain.addSource(std::make_shared<AddDeduplicationInfoTransform>(
                insert_dependencies,
                insert_dependencies->getRootViewID(),
                settings[Setting::insert_deduplication_token].value,
                sink_chain.getInputSharedHeader()));

        if (should_squash)
            sink_chain.addSource(std::make_shared<ApplySquashingTransform>(sink_chain.getInputSharedHeader()));
    }

    /// Distribute the single input stream across the parallel insert streams.
    std::vector<OutputPort *> stream_outputs;
    if (sink_stream_size > 1)
    {
        auto resize = std::make_shared<ResizeProcessor>(head_output->getSharedHeader(), 1, sink_stream_size);
        connect(*head_output, resize->getInputs().front());
        for (auto & output : resize->getOutputs())
            stream_outputs.push_back(&output);
        processors->emplace_back(std::move(resize));
    }
    else
    {
        stream_outputs.push_back(head_output);
    }

    chassert(stream_outputs.size() == sink_chains.size());

    /// Connect each parallel stream to its sink chain and terminate it with an empty sink.
    QueryPlanResourceHolder resources;
    size_t stream_index = 0;
    for (auto & sink_chain : sink_chains)
    {
        connect(*stream_outputs[stream_index], sink_chain.getInputPort());
        ++stream_index;

        auto sink = std::make_shared<EmptySink>(sink_chain.getOutputSharedHeader());
        connect(sink_chain.getOutputPort(), sink->getPort());

        for (auto processor : sink_chain.getProcessors())
            processors->emplace_back(std::move(processor));
        processors->emplace_back(std::move(sink));

        resources = sink_chain.detachResources();
    }

    QueryPipeline pipeline(std::move(resources), std::move(processors), pipeline_input);

    // Pipeline ceiling: simple upper bound on parallelism. Actual slot grants are
    // demand-driven by lazy ConcurrencyControl / CPULeaseAllocation, so a wide ceiling
    // does not translate into reserved-but-unused slots.
    // max_threads is already memory-adjusted; use it for the parallel case to preserve that adjustment.
    const bool serial_views = !settings[Setting::parallel_view_processing] && insert_dependencies->isViewsInvolved();
    pipeline.setNumThreads(serial_views ? 1 : max_threads);
    pipeline.setConcurrencyControl(settings[Setting::use_concurrency_control]);

    if (query.hasInlinedData() && !async_insert)
    {
        auto format = getInputFormatFromASTInsertQuery(query_ptr, true, *query_sample_block, context, nullptr);

        if (settings[Setting::enable_parsing_to_custom_serialization])
            format->setSerializationHints(table->getSerializationHints());

        auto pipe = getSourceFromInputFormat(query_ptr, std::move(format), context, nullptr);
        pipeline.complete(std::move(pipe));
    }

    return pipeline;
}

std::optional<QueryPipeline> InterpreterInsertQuery::distributedWriteIntoReplicatedMergeTreeOrDataLakeFromClusterStorage(
    const ASTInsertQuery & query, ContextPtr local_context)
{
    if (query.table_id.empty())
        return {};

    StoragePtr dst_storage = DatabaseCatalog::instance().getTable(query.table_id, local_context);
    if (!(dst_storage->isMergeTree() || dst_storage->isDataLake()) || !dst_storage->supportsReplication())
        return {};

    auto & select = query.select->as<ASTSelectWithUnionQuery &>();
    StoragePtr src_storage;
    const ASTSelectQuery * select_query = nullptr;
    if (select.list_of_selects->children.size() == 1)
    {
        if (auto * sq = select.list_of_selects->children.at(0)->as<ASTSelectQuery>())
        {
            select_query = sq;
            if (local_context->getSettingsRef()[Setting::enable_global_with_statement])
                ApplyWithAliasVisitor::visit(select.list_of_selects->children.at(0));
            ApplyWithSubqueryVisitor::visit(select.list_of_selects->children.at(0));

            JoinedTables joined_tables(Context::createCopy(local_context), *sq);
            if (joined_tables.tablesCount() == 1)
                src_storage = joined_tables.getLeftTableStorage();
        }
    }
    if (!src_storage)
        return {};

    auto src_storage_cluster = std::dynamic_pointer_cast<IStorageCluster>(src_storage);
    if (!src_storage_cluster)
        return {};

    if (!isInsertSelectTrivialEnoughForDistributedExecution(query))
        return {};

    /// Do not enable parallel distributed INSERT SELECT in case when query probably comes from another server
    if (local_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return {};

    const Settings & settings = local_context->getSettingsRef();
    if (settings[Setting::max_distributed_depth]
        && local_context->getClientInfo().distributed_depth >= settings[Setting::max_distributed_depth])
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    /// query will be executed on all nodes of the cluster
    auto src_cluster = src_storage_cluster->getCluster(local_context);

    /// Actually the query doesn't change, we just serialize it to string. Strip the initiator-only
    /// settings from the forwarded query text (both `changes` and `default_settings`, across the INSERT
    /// and its source SELECT) so those names — including the new HTTP table-as-file settings — do not reach
    /// the shards and trip `UNKNOWN_SETTING` on a rolling upgrade; the per-shard context is stripped below.
    auto query_to_send = query.clone();
    ClusterProxy::stripInitiatorOnlySettingsFromQuery(query_to_send);
    String query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(
            /*one_line=*/true, /*identifier_quoting_rule=*/IdentifierQuotingRule::Always);
        query_to_send->IAST::format(buf, ast_format_settings);
        query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();
    query_context->setSetting("skip_unavailable_shards", true);
    /// Same contract as the other remote paths: the inter-server settings packet must not carry the
    /// initiator-only settings either.
    {
        Settings stripped_settings = query_context->getSettingsRef();
        ClusterProxy::stripInitiatorOnlySettings(stripped_settings);
        query_context->setSettings(stripped_settings);
    }

    src_storage_cluster->updateExternalDynamicMetadataIfExists(local_context);

    const auto src_metadata_snapshot = src_storage_cluster->getInMemoryMetadataPtr(local_context, false);

    std::optional<ActionsDAG> filter_dag;
    const ActionsDAG::Node * predicate = nullptr;
    if (select_query && (select_query->prewhere() || select_query->where()))
    {
        /// The metadata and the snapshot are acquired outside of the `try` block below:
        /// a failure here is a real storage-side problem rather than an expected miss of
        /// the best-effort condition analysis, so it has to propagate.
        const auto snapshot = src_storage_cluster->getStorageSnapshot(src_metadata_snapshot, local_context);
        const auto columns = snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All));

        try
        {
            /// `PREWHERE` and `WHERE` can reference aliases introduced in the `WITH` clause or in the `SELECT` list,
            /// as in `WITH splitByChar(' ', line) AS values SELECT ... WHERE length(values) >= 3`.
            /// The condition is analyzed here in isolation from the rest of the query, so the aliases have to be
            /// substituted first - otherwise the analysis below would not be able to resolve them.
            /// It is done on a copy, because the original AST has already been serialized for the remote nodes.
            NameSet source_columns_set;
            for (const auto & column : columns)
                source_columns_set.insert(column.name);

            ASTPtr select_copy = select_query->clone();
            Aliases aliases;
            QueryAliasesVisitor(aliases).visit(select_copy);
            MarkTableIdentifiersVisitor::Data mark_identifiers_data{aliases};
            MarkTableIdentifiersVisitor(mark_identifiers_data).visit(select_copy);
            QueryNormalizer::Data normalizer_data(
                aliases,
                source_columns_set,
                /*ignore_alias_=*/ false,
                QueryNormalizer::ExtractedSettings(settings),
                /*allow_self_aliases_=*/ true);
            QueryNormalizer(normalizer_data).visit(select_copy);

            const auto & normalized_select = select_copy->as<ASTSelectQuery &>();

            ASTPtr condition_ast;
            if (normalized_select.prewhere() && normalized_select.where())
                condition_ast = makeASTOperator("and", normalized_select.prewhere()->clone(), normalized_select.where()->clone());
            else if (normalized_select.prewhere())
                condition_ast = normalized_select.prewhere()->clone();
            else
                condition_ast = normalized_select.where()->clone();

            auto syntax = TreeRewriter(local_context).analyze(condition_ast, columns);
            filter_dag = ExpressionAnalyzer(condition_ast, syntax, local_context).getActionsDAG(true, true);
            predicate = filter_dag->getOutputs().at(0);
        }
        catch (...)
        {
            /// Filter extraction is best-effort: the condition is analyzed here in isolation
            /// from the rest of the query, so the analysis can legitimately fail (e.g. the
            /// predicate references columns qualified with a table alias, which is not
            /// resolvable in this isolated pass). Fall back to no pruning so the query still
            /// executes correctly. This is an expected outcome for some queries rather than
            /// an error, hence the low log level. A logical error, however, indicates a bug
            /// rather than an expected miss, so it is logged prominently.
            tryLogCurrentException(
                logger,
                "Cannot build the filter expression for pruning in INSERT ... SELECT; continuing without pruning",
                getCurrentExceptionCode() == ErrorCodes::LOGICAL_ERROR ? LogsLevel::error : LogsLevel::debug);
            filter_dag.reset();
            predicate = nullptr;
        }
    }
    auto extension = src_storage_cluster->getTaskIteratorExtension(
        predicate, filter_dag ? &*filter_dag : nullptr, local_context, src_cluster, src_metadata_snapshot);

    /// -Cluster storage treats each replicas as a shard in cluster definition
    /// so, it's enough to consider only shards here
    size_t replica_index = 0;
    for (const auto & shard : src_cluster->getShardsInfo())
    {
        auto pools = shard.pool->getShuffledPools(settings);
        chassert(pools.size() == 1);

        IConnections::ReplicaInfo replica_info{.number_of_current_replica = replica_index++};
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            pools.at(0).pool,
            query_str,
            std::make_shared<const Block>(Block{}),
            query_context,
            /*throttler=*/nullptr,
            Scalars{},
            Tables{},
            QueryProcessingStage::Complete,
            RemoteQueryExecutor::Extension{.task_iterator = extension.task_iterator, .replica_info = std::move(replica_info)});
        remote_query_executor->setLogger(logger);
        /// check if destination table exists on nodes
        remote_query_executor->setMainTable(dst_storage->getStorageID());

        Pipe pipe{std::make_shared<RemoteSource>(
            remote_query_executor, false, settings[Setting::async_socket_for_remote], settings[Setting::async_query_sending_for_remote])};
        pipe.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<UnmarshallBlocksTransform>(header); });
        QueryPipeline remote_pipeline{std::move(pipe)};
        remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getSharedHeader()));

        pipeline.addCompletedPipeline(std::move(remote_pipeline));
    }

    return pipeline;
}


BlockIO InterpreterInsertQuery::execute()
{
    auto context = getContext();
    const Settings & settings = context->getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    StoragePtr table = getTable(query);
    setInsertContextValues(context, query, table);
    if (context->getServerSettings()[ServerSetting::disable_insertion_and_mutation]
        && query.table_id.database_name != DatabaseCatalog::SYSTEM_DATABASE
        && query.table_id.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        /// Allow inserts that write out to external storage (object storage, message queues,
        /// external databases): they create no merge tasks on this replica.
        /// Background streaming pushes (`no_destination`) skip the external table and feed attached
        /// materialized views instead, producing `MergeTree` parts, so they are not exempt.
        bool writes_out_to_external_storage = !no_destination
            && (table->isObjectStorage() || table->isDataLake()
                || table->isMessageQueue() || table->isExternalDatabase());

        if (!writes_out_to_external_storage)
            throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Insert queries are prohibited");
    }

    if (context->getMessageQueueDisableInsertion()
        && table->isMessageQueue()
        && no_destination)
    {
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Message queue insertion is disabled");
    }

    checkStorageSupportsTransactionsIfNeeded(table, getContext());

    if (query.partition_by && !table->supportsPartitionBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PARTITION BY clause is not supported by storage");

    auto table_lock = table->lockForShare(context->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);

    table->updateExternalDynamicMetadataIfExists(context);
    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, context, no_destination, allow_materialized);
    /// For table functions we check access while executing
    /// getTable() -> ITableFunction::execute().
    /// `skip_target_insert_access_check` is set only for the internal populate of `CREATE TABLE ... AS
    /// SELECT` into a temporary `_tmp_replace_*` table; the final-name `INSERT` privilege is verified up
    /// front by the caller, so re-authorizing `INSERT` on the meaningless temporary name would be a
    /// spurious `ACCESS_DENIED` for table-scoped grants. Source `SELECT` access is still checked below.
    if (!query.table_function && !skip_target_insert_access_check)
        context->checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

    /// Access the storage itself guards the write with (e.g. the source access of a table of a
    /// `URL` database). It is also checked when the sink is created, but that happens in a
    /// background flush for asynchronous inserts, so the check has to be repeated here.
    if (!query.table_function)
        table->checkInsertIsAllowed(context);

    if (!allow_materialized)
    {
        for (const auto & column : metadata_snapshot->getColumns())
            if (column.default_desc.kind == ColumnDefaultKind::Materialized && query_sample_block.has(column.name))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert column {}, because it is MATERIALIZED column.", column.name);
    }

    BlockIO res;
    if (query.select)
    {
        if (settings[Setting::parallel_distributed_insert_select])
        {
            /// distributed write paths may mutate the SELECT AST (CTE expansion), so keep a backup
            auto saved_select = query.select->clone();

            auto distributed = table->distributedWrite(query, context);
            if (distributed)
            {
                res.pipeline = std::move(*distributed);
            }
            if (!res.pipeline.initialized())
            {
                if (auto pipeline = distributedWriteIntoReplicatedMergeTreeOrDataLakeFromClusterStorage(query, context); pipeline)
                    res.pipeline = std::move(*pipeline);
            }
            if (!res.pipeline.initialized())
            {
                auto pipeline = buildInsertSelectPipelineParallelReplicas(query, table);
                if (pipeline)
                    res.pipeline = std::move(*pipeline);
            }

            query.select = std::move(saved_select);
        }
        if (!res.pipeline.initialized())
            res.pipeline = buildInsertSelectPipeline(query, table);
    }
    else
    {
        res.pipeline = buildInsertPipeline(query, table);
    }

    res.pipeline.addStorageHolder(table);

    /// Keep the share lock until the pipeline finishes (not just while it is being built), so that
    /// the dependent-view discovery and the commit of the inserted data are indivisible with respect
    /// to an exclusive lock on the table. The atomic `CREATE MATERIALIZED VIEW ... POPULATE` relies
    /// on this: under its brief exclusive lock on the source, any concurrent `INSERT` has either
    /// already committed (and is covered by the pinned snapshot) or has not yet discovered the
    /// dependent views (and will see the newly registered view).
    QueryPlanResourceHolder insert_resources;
    insert_resources.table_locks.emplace_back(std::move(table_lock));
    res.pipeline.addResources(std::move(insert_resources));

    if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table.get()))
        res.pipeline.addStorageHolder(mv->getTargetTable());

    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}

void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, ContextPtr context_)
{
    const auto & insert_table = context_->getInsertionTable();
    if (!insert_table.empty())
    {
        elem.query_databases.insert(backQuoteIfNeed(insert_table.getDatabaseName()));
        elem.query_tables.insert(insert_table.getFullTableName());
    }
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    extendQueryLogElemImpl(elem, context_);
}

void InterpreterInsertQuery::setInsertContextValues(ContextMutablePtr context_, const ASTInsertQuery & insert_query, const StoragePtr & table)
{
    const auto metadata_snapshot = table->getInMemoryMetadataPtr(context_, false);
    std::optional<Names> insert_columns;
    if (insert_query.columns)
    {
        const auto columns_ast = processColumnTransformers(context_->getCurrentDatabase(), table, metadata_snapshot, insert_query.columns);
        Names names;
        names.reserve(columns_ast->children.size());
        for (const auto & identifier : columns_ast->children)
        {
            std::string current_name = identifier->getColumnName();
            names.emplace_back(std::move(current_name));
        }

        insert_columns = std::move(names);
    }

    context_->setInsertionTable(insert_query.table_id, insert_columns, std::make_shared<ColumnsDescription>(metadata_snapshot->columns));
}

void registerInterpreterInsertQuery(InterpreterFactory & factory);
void registerInterpreterInsertQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterInsertQuery>(
            args.query,
            args.context,
            args.allow_materialized,
            /* no_squash */false,
            /* no_destination */false,
            /* async_insert */false);
    };
    factory.registerInterpreter("InterpreterInsertQuery", create_fn);
}


}
