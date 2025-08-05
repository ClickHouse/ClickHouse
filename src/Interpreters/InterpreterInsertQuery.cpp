#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledQuota.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/processColumnTransformers.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTIdentifier.h>
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
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <Common/ProfileEvents.h>

#include <memory>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool distributed_foreground_insert;
    extern const SettingsBool insert_null_as_default;
    extern const SettingsBool optimize_trivial_insert_select;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_insert_threads;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsString insert_deduplication_token;
    extern const SettingsBool parallel_view_processing;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 parallel_distributed_insert_select;
    extern const SettingsBool enable_parsing_to_custom_serialization;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsBool parallel_replicas_insert_select_local_pipeline;
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
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_, bool async_insert_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
    , async_insert(async_insert_)
{
    checkStackSize();
    if (auto quota = getContext()->getQuota())
        quota->checkExceeded(QuotaType::WRITTEN_BYTES);

    const Settings & settings = getContext()->getSettingsRef();
    max_threads = std::max<size_t>(1, settings[Setting::max_threads]);
    max_insert_threads = std::min(std::max<size_t>(1, settings[Setting::max_insert_threads]), max_threads);
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
            Block header_block;
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
                header_block = tmp_pipeline.getHeader();
            }

            ColumnsDescription structure_hint{header_block.getNamesAndTypesList()};
            table_function_ptr->setStructureHint(structure_hint);
        }

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
        if (auto * window_view = dynamic_cast<StorageWindowView *>(table.get()))
            return window_view->getInputHeader();
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtualsList());
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

std::optional<Names> InterpreterInsertQuery::getInsertColumnNames() const
{
    auto const * insert_query = query_ptr->as<ASTInsertQuery>();
    if (!insert_query || !insert_query->columns)
        return std::nullopt;

    auto table = DatabaseCatalog::instance().getTable(getDatabaseTable(), getContext());
    const auto columns_ast = processColumnTransformers(getContext()->getCurrentDatabase(), table, table->getInMemoryMetadataPtr(), insert_query->columns);
    Names names;
    names.reserve(columns_ast->children.size());
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();
        names.emplace_back(std::move(current_name));
    }

    return names;
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
            table_sample_virtuals = table->getVirtualsHeader();

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

void InterpreterInsertQuery::addBuffer(std::unique_ptr<ReadBuffer> buffer)
{
    owned_buffers.push_back(std::move(buffer));
}

bool InterpreterInsertQuery::shouldAddSquashingForStorage(const StoragePtr & table, ContextPtr context_)
{
    const Settings & settings = context_->getSettingsRef();

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    return !(settings[Setting::distributed_foreground_insert] && table->isRemote());
}

static std::pair<QueryPipelineBuilder, ParallelReplicasReadingCoordinatorPtr> getLocalSelectPipelineForInserSelectWithParallelReplicas(const ASTPtr & select, const ContextPtr & context)
{
    auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, /*subquery_depth_=*/1);

    InterpreterSelectQueryAnalyzer interpreter(select, context, select_query_options);
    auto & plan = interpreter.getQueryPlan();

    /// Find reading steps for remote replicas and remove them,
    /// When building local pipeline, the local replica will be registered in the returned coordinator,
    /// and announce its snapshot. The snapshot will be used to assign read tasks to involved replicas
    /// So, the remote pipelines, which will be created later, should use the same coordinator
    auto parallel_replicas_coordinator = ClusterProxy::dropReadFromRemoteInPlan(plan);
    return  {interpreter.buildQueryPipeline(), parallel_replicas_coordinator};
}


QueryPipeline InterpreterInsertQuery::addInsertToSelectPipeline(ASTInsertQuery & query, StoragePtr table, QueryPipelineBuilder & pipeline)
{
    const Settings & settings = getContext()->getSettingsRef();

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, getContext(), no_destination, allow_materialized);

    pipeline.dropTotalsAndExtremes();

    /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
    if (getContext()->getSettingsRef()[Setting::insert_null_as_default])
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
            ActionsDAG::MatchColumnsMode::Position);
    auto actions = std::make_shared<ExpressionActions>(std::move(actions_dag), ExpressionActionsSettings(getContext(), CompileExpressions::yes));

    pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(in_header, actions);
    });

    pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
    {
        auto context_ptr = getContext();
        auto counting = std::make_shared<CountingTransform>(in_header, context_ptr->getQuota());
        counting->setProcessListElement(context_ptr->getProcessListElement());
        counting->setProgressCallback(context_ptr->getProgressCallback());

        return counting;
    });

    pipeline.resize(1);

    if (shouldAddSquashingForStorage(table, getContext()) && !no_squash && !async_insert)
    {
        pipeline.addSimpleTransform(
            [&](const Block & in_header) -> ProcessorPtr
            {
                return std::make_shared<PlanSquashingTransform>(
                    in_header,
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_bytes] : 0ULL);
            });
    }

    pipeline.addSimpleTransform([&](const Block &in_header) -> ProcessorPtr
    {
        return std::make_shared<DeduplicationToken::AddTokenInfoTransform>(in_header);
    });

    if (!settings[Setting::insert_deduplication_token].value.empty())
    {
        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<DeduplicationToken::SetUserTokenTransform>(settings[Setting::insert_deduplication_token].value, in_header);
        });

        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<DeduplicationToken::SetSourceBlockNumberTransform>(in_header);
        });
    }

    auto insert_dependencies = InsertDependenciesBuilder::create(
        table, query_ptr, query_sample_block,
        async_insert, /*skip_destination_table*/ no_destination,
        getContext());

    size_t sink_streams_size = table->supportsParallelInsert() ? max_insert_threads : 1;

    if (!settings[Setting::parallel_view_processing] && insert_dependencies->isViewsInvolved())
    {
        sink_streams_size = 1;
    }

    std::vector<Chain> sink_chains;
    for (size_t i = 0; i < sink_streams_size; ++i)
    {
        sink_chains.push_back(insert_dependencies->createChainWithDependencies());
    }

    pipeline.resize(sink_streams_size);

    if (shouldAddSquashingForStorage(table, getContext()) && !no_squash && !async_insert)
    {
        pipeline.addSimpleTransform(
            [&](const Block & in_header) -> ProcessorPtr
            {
                return std::make_shared<ApplySquashingTransform>(in_header);
            });
    }

    for (auto & chain : sink_chains)
    {
        pipeline.addResources(chain.detachResources());
    }
    pipeline.addChains(std::move(sink_chains));

    pipeline.setMaxThreads(max_threads);

    pipeline.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<EmptySink>(cur_header);
    });

    return QueryPipelineBuilder::getPipeline(std::move(pipeline));
}

static void applyTrivialInsertSelectOptimization(ASTInsertQuery & query, bool prefer_large_blocks, ContextPtr & select_context)
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

        new_settings[Setting::max_threads] = std::max<UInt64>(1, settings[Setting::max_insert_threads]);

        if (prefer_large_blocks)
        {
            if (settings[Setting::min_insert_block_size_rows])
                new_settings[Setting::max_block_size] = settings[Setting::min_insert_block_size_rows];
            if (settings[Setting::min_insert_block_size_bytes])
                new_settings[Setting::preferred_block_size_bytes] = settings[Setting::min_insert_block_size_bytes];
        }

        auto context_for_trivial_select = Context::createCopy(select_context);
        context_for_trivial_select->setSettings(new_settings);
        context_for_trivial_select->setInsertionTable(select_context->getInsertionTable(), select_context->getInsertionTableColumnNames());

        select_context = context_for_trivial_select;
    }
}


QueryPipeline InterpreterInsertQuery::buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table)
{
    ContextPtr select_context = getContext();
    applyTrivialInsertSelectOptimization(query, table->prefersLargeBlocks(), select_context);

    QueryPipelineBuilder pipeline;

    {
        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

        const Settings & settings = select_context->getSettingsRef();
        if (settings[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter_select_analyzer(query.select, select_context, select_query_options);
            pipeline = interpreter_select_analyzer.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectWithUnionQuery interpreter_select(query.select, select_context, select_query_options);
            pipeline = interpreter_select.buildQueryPipeline();
        }
    }

    return addInsertToSelectPipeline(query, table, pipeline);
}


std::pair<QueryPipeline, ParallelReplicasReadingCoordinatorPtr>
InterpreterInsertQuery::buildLocalInsertSelectPipelineForParallelReplicas(ASTInsertQuery & query, const StoragePtr & table)
{
    ContextPtr select_context = getContext();
    applyTrivialInsertSelectOptimization(query, table->prefersLargeBlocks(), select_context);

    auto [pipeline_builder, coordinator]
        = getLocalSelectPipelineForInserSelectWithParallelReplicas(query.select, select_context);
    auto local_pipeline = addInsertToSelectPipeline(query, table, pipeline_builder);
    return {std::move(local_pipeline), coordinator};
}


std::optional<QueryPipeline> InterpreterInsertQuery::buildInsertSelectPipelineParallelReplicas(ASTInsertQuery & query, StoragePtr table)
{
    auto context_ptr = getContext();
    const Settings & settings = context_ptr->getSettingsRef();
    if (!settings[Setting::allow_experimental_analyzer])
        return {};

    if (!context_ptr->canUseParallelReplicasOnInitiator())
        return {};

    if (settings[Setting::parallel_distributed_insert_select] != 2)
        return {};

    const auto & select_query = query.select->as<ASTSelectWithUnionQuery &>();
    const auto & selects = select_query.list_of_selects->children;
    if (selects.size() > 1)
        return {};

    if (!isTrivialSelect(selects.front()))
        return {};

    if (!ClusterProxy::isSuitableForParallelReplicas(selects.front(), context_ptr))
        return {};

    LOG_TRACE(
        getLogger("InterpreterInsertQuery"),
        "Building distributed insert select pipeline with parallel replicas: table={}",
        query.getTable());

    if (settings[Setting::parallel_replicas_local_plan] && settings[Setting::parallel_replicas_insert_select_local_pipeline])
    {
        auto [local_pipeline, coordinator] = buildLocalInsertSelectPipelineForParallelReplicas(query, table);
        return ClusterProxy::executeInsertSelectWithParallelReplicas(query, context_ptr, std::move(local_pipeline), coordinator);
    }

    return ClusterProxy::executeInsertSelectWithParallelReplicas(query, context_ptr);
}


QueryPipeline InterpreterInsertQuery::buildInsertPipeline(ASTInsertQuery & query, StoragePtr table)
{
    const Settings & settings = getContext()->getSettingsRef();

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, getContext(), no_destination, allow_materialized);

    // when insert is initiated from FileLog or similar storages
    // they are allowed to expose its virtuals columns to the dependent views
    auto insert_dependencies = InsertDependenciesBuilder::create(
        table, query_ptr, query_sample_block,
        async_insert, /*skip_destination_table*/ no_destination,
        getContext());

    Chain chain = insert_dependencies->createChainWithDependencies();

    if (!settings[Setting::insert_deduplication_token].value.empty())
    {
        chain.addSource(std::make_shared<DeduplicationToken::SetSourceBlockNumberTransform>(chain.getInputHeader()));
        chain.addSource(std::make_shared<DeduplicationToken::SetUserTokenTransform>(
            settings[Setting::insert_deduplication_token].value, chain.getInputHeader()));
    }

    chain.addSource(std::make_shared<DeduplicationToken::AddTokenInfoTransform>(chain.getInputHeader()));

    if (shouldAddSquashingForStorage(table, getContext()) && !no_squash && !async_insert)
    {
        bool table_prefers_large_blocks = table->prefersLargeBlocks();

        auto applying = std::make_shared<ApplySquashingTransform>(chain.getInputHeader());
        chain.addSource(std::move(applying));

        auto planing = std::make_shared<PlanSquashingTransform>(
            chain.getInputHeader(),
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL);
        chain.addSource(std::move(planing));
    }

    auto context_ptr = getContext();
    auto counting = std::make_shared<CountingTransform>(chain.getInputHeader(), context_ptr->getQuota());
    counting->setProcessListElement(context_ptr->getProcessListElement());
    counting->setProgressCallback(context_ptr->getProgressCallback());
    chain.addSource(std::move(counting));

    QueryPipeline pipeline = QueryPipeline(std::move(chain));

    pipeline.setNumThreads(max_insert_threads);
    pipeline.setConcurrencyControl(settings[Setting::use_concurrency_control]);

    if (query.hasInlinedData() && !async_insert)
    {
        auto format = getInputFormatFromASTInsertQuery(query_ptr, true, query_sample_block, getContext(), nullptr);

        for (auto & buffer : owned_buffers)
            format->addBuffer(std::move(buffer));

        if (settings[Setting::enable_parsing_to_custom_serialization])
            format->setSerializationHints(table->getSerializationHints());

        auto pipe = getSourceFromInputFormat(query_ptr, std::move(format), getContext(), nullptr);
        pipeline.complete(std::move(pipe));
    }

    return pipeline;
}


BlockIO InterpreterInsertQuery::execute()
{
    auto context = getContext();
    const Settings & settings = context->getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    if (context->getServerSettings()[ServerSetting::disable_insertion_and_mutation]
        && query.table_id.database_name != DatabaseCatalog::SYSTEM_DATABASE
        && query.table_id.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Insert queries are prohibited");
    }

    StoragePtr table = getTable(query);

    checkStorageSupportsTransactionsIfNeeded(table, getContext());

    if (query.partition_by && !table->supportsPartitionBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PARTITION BY clause is not supported by storage");

    auto table_lock = table->lockForShare(context->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, context, no_destination, allow_materialized);

    /// For table functions we check access while executing
    /// getTable() -> ITableFunction::execute().
    if (!query.table_function)
        context->checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

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
            auto distributed = table->distributedWrite(query, context);
            if (distributed)
            {
                res.pipeline = std::move(*distributed);
            }
            if (!res.pipeline.initialized() && context->canUseParallelReplicasOnInitiator())
            {
                auto pipeline = buildInsertSelectPipelineParallelReplicas(query, table);
                if (pipeline)
                    res.pipeline = std::move(*pipeline);
            }
        }
        if (!res.pipeline.initialized())
            res.pipeline = buildInsertSelectPipeline(query, table);
    }
    else
    {
        res.pipeline = buildInsertPipeline(query, table);
    }

    res.pipeline.addStorageHolder(table);

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
        elem.query_databases.insert(insert_table.getDatabaseName());
        elem.query_tables.insert(insert_table.getFullNameNotQuoted());
    }
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    extendQueryLogElemImpl(elem, context_);
}


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
