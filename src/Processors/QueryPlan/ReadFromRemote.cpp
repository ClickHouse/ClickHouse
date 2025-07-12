#include <Processors/QueryPlan/ReadFromRemote.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Planner/PlannerActionsVisitor.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PredicateRewriteVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <Common/FailPoint.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <fmt/format.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool extremes;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 parallel_replicas_mark_segment_size;
    extern const SettingsBool allow_push_predicate_when_subquery_contains_with;
    extern const SettingsBool enable_optimize_predicate_expression_to_final_subquery;
    extern const SettingsBool allow_push_predicate_ast_for_distributed_subqueries;
    extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace FailPoints
{
    extern const char use_delayed_remote_source[];
    extern const char parallel_replicas_wait_for_unused_replicas[];
}

static void addConvertingActions(Pipe & pipe, const Block & header, bool use_positions_to_match = false)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    auto match_mode = use_positions_to_match ? ActionsDAG::MatchColumnsMode::Position : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode = match_mode](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
            true);
    };

    if (use_positions_to_match)
        pipe.addSimpleTransform([](const Block & stream_header) { return std::make_shared<MaterializingTransform>(stream_header); });

    auto convert_actions = std::make_shared<ExpressionActions>(get_converting_dag(pipe.getHeader(), header));
    pipe.addSimpleTransform([&](const Block & cur_header, Pipe::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(cur_header, convert_actions);
    });
}

static void enableMemoryBoundMerging(QueryProcessingStage::Enum stage, const ClusterProxy::SelectStreamFactory::Shards * shards, Context & context)
{
    if (stage != QueryProcessingStage::WithMergeableState)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot enforce sorting for ReadFromRemote step up to stage {}",
            QueryProcessingStage::toString(stage));

    context.setSetting("enable_memory_bound_merging_of_aggregation_results", true);

    if (!shards)
        return;

    for (const auto & shard : *shards)
    {
        if (!shard.query_plan)
            continue;

        auto * aggregating_step = typeid_cast<AggregatingStep *>(shard.query_plan->getRootNode()->step.get());
        if (!aggregating_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot enable memory bound merging because the last step of remote plan is not Aggregating ({})",
                shard.query_plan->getRootNode()->step->getName());

        aggregating_step->enableMemoryBoundMerging();
    }
}

static void enforceAggregationInOrder(
    QueryProcessingStage::Enum stage,
    const ClusterProxy::SelectStreamFactory::Shards * shards,
    const SortDescription & sort_description,
    Context & context)
{
    if (stage != QueryProcessingStage::WithMergeableState)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot enforce aggregation in order for ReadFromRemote step up to stage {}",
            QueryProcessingStage::toString(stage));

    context.setSetting("optimize_aggregation_in_order", true);
    context.setSetting("force_aggregation_in_order", true);

    if (!shards)
        return;

    for (const auto & shard : *shards)
    {
        if (!shard.query_plan)
            continue;

        auto sorting = std::make_unique<SortingStep>(shard.query_plan->getCurrentHeader(), sort_description, 0, SortingStep::Settings(context.getSettingsRef()));
        sorting->setStepDescription("Enforce aggregation in order");
        shard.query_plan->addStep(std::move(sorting));
    }
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(
        /*one_line=*/true, /*hilite=*/false, /*identifier_quoting_rule=*/IdentifierQuotingRule::Always);
    ast->format(buf, ast_format_settings);
    return buf.str();
}

ReadFromRemote::ReadFromRemote(
    ClusterProxy::SelectStreamFactory::Shards shards_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ContextMutablePtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    LoggerPtr log_,
    UInt32 shard_count_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    const String & cluster_name_)
    : SourceStepWithFilterBase(std::move(header_))
    , shards(std::move(shards_))
    , stage(stage_)
    , main_table(std::move(main_table_))
    , table_func_ptr(std::move(table_func_ptr_))
    , context(std::move(context_))
    , throttler(std::move(throttler_))
    , scalars(std::move(scalars_))
    , external_tables(std::move(external_tables_))
    , storage_limits(std::move(storage_limits_))
    , log(log_)
    , shard_count(shard_count_)
    , cluster_name(cluster_name_)
{
}

void ReadFromRemote::enableMemoryBoundMerging()
{
    DB::enableMemoryBoundMerging(stage, &shards, *context);
}

void ReadFromRemote::enforceAggregationInOrder(const SortDescription & sort_description)
{
    DB::enforceAggregationInOrder(stage, &shards, sort_description, *context);
}

ASTSelectQuery & getSelectQuery(ASTPtr ast)
{
    if (const auto * explain = ast->as<ASTExplainQuery>())
        ast = explain->getExplainedQuery();

    return ast->as<ASTSelectQuery &>();
}

/// This is an attempt to convert filters (pushed down from the plan optimizations) from ActionsDAG back to AST.
/// It should not be needed after we send a full plan for distributed queries.
static ASTPtr tryBuildAdditionalFilterAST(
    const ActionsDAG & dag,
    const std::unordered_set<std::string> & projection_names,
    const std::unordered_map<std::string, QueryTreeNodePtr> & execution_name_to_projection_query_tree,
    Tables * external_tables,
    const ContextPtr & context)
{
    std::unordered_map<const ActionsDAG::Node *, ASTPtr> node_to_ast;

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child = 0;
    };
    std::stack<Frame> stack;
    stack.push({dag.getOutputs().front()});
    while (!stack.empty())
    {
        auto & frame = stack.top();
        const auto * node = frame.node;

        if (node_to_ast.contains(node))
        {
            stack.pop();
            continue;
        }

        /// Labmdas are not supported (converting back to AST is complicated).
        /// We have two cases here cause function with no capture can be constant-folded.
        if (WhichDataType(node->result_type).isFunction()
            || (node->type == ActionsDAG::ActionType::FUNCTION
                && typeid_cast<const FunctionCapture *>(node->function_base.get())))
        {
            node_to_ast[node] = nullptr;
            stack.pop();
            continue;
        }

        /// Support for IN. The stored AST from the Set is taken.
        if (WhichDataType(node->result_type).isSet())
        {
            auto maybe_set = node->column;
            if (const auto * col_const = typeid_cast<const ColumnConst *>(maybe_set.get()))
                maybe_set = col_const->getDataColumnPtr();

            if (const auto * col_set = typeid_cast<const ColumnSet *>(maybe_set.get()))
                node_to_ast[node] = col_set->getData()->getSourceAST();

            stack.pop();
            continue;
        }

        if (node->column && isColumnConst(*node->column))
        {
            auto literal = std::make_shared<ASTLiteral>((*node->column)[0]);
            /// Need to enforce type of the literal, because some type is not comparable to its native type
            /// E.g. `Date` has native type `UInt32`, but comparing `Date` with `UInt32` is not allowed.
            auto casted_literal = makeASTFunction("_CAST", literal, std::make_shared<ASTLiteral>(node->result_type->getName()));
            node_to_ast[node] = std::move(casted_literal);
            stack.pop();
            continue;
        }

        if (frame.next_child < node->children.size())
        {
            stack.push({node->children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        stack.pop();
        auto & res = node_to_ast[node];

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            /// The column name can be taken from the projection name, or from the projection expression.
            /// It depends on the predicate and query stage.

            if (projection_names.contains(node->result_name))
            {
                /// The input name matches the projection name. Example:
                /// SELECT x FROM (SELECT number + 1 AS x FROM remote('127.0.0.2', numbers(3))) WHERE x = 1
                /// In this case, ReadFromRemote has header `x UInt64` and filter DAG has input column with name `x`.
                /// Here, filter is applied to the whole query, and checking for projection name is reasonable.
                res = std::make_shared<ASTIdentifier>(node->result_name);
            }
            else
            {
                /// The input name matches the execution name of the projection. Example:
                /// SELECT x, y FROM (
                ///     SELECT number + 1 AS x, sum(number) AS y FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY x
                /// ) WHERE x = 1
                /// In this case, ReadFromRemote has `plus(__table1.number, 1_UInt8) UInt64` in header,
                /// and filter DAG has input column with name `plus(__table1.number, 1_UInt8)`.
                /// Here, filter is pushed down before the aggregation, and projection name can't be used.
                /// However, we can match the input with the execution name of projection query tree.
                /// Note: this may not cover all the cases.
                auto it = execution_name_to_projection_query_tree.find(node->result_name);
                if (it != execution_name_to_projection_query_tree.end())
                    /// Append full expression as an AST.
                    /// We rely on plan optimization that the result is (expected to be) valid.
                    res = it->second->toAST();
            }
        }

        if (node->type == ActionsDAG::ActionType::ALIAS)
            res = node_to_ast[node->children.at(0)];

        if (node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        if (!node->function_base->isDeterministic() || node->function_base->isStateful())
            continue;

        bool has_all_args = true;
        ASTs arguments;
        for (const auto * child : node->children)
        {
            auto ast = node_to_ast[child];
            if (!ast)
                has_all_args = false;
            else
                arguments.push_back(std::move(ast));
        }

        /// Allow to skip children only for AND function.
        auto func_name = node->function_base->getName();
        bool is_function_and = func_name == "and";
        if (!has_all_args && !is_function_and)
            continue;

        if (is_function_and && arguments.empty())
            continue;

        /// and() with 1 arg is not allowed. Make it AND(condition, 1)
        if (is_function_and && arguments.size() == 1)
            arguments.push_back(std::make_shared<ASTLiteral>(Field(1)));

        /// Support for GLOBAL IN.
        if (external_tables && isNameOfGlobalInFunction(func_name))
        {
            const auto * second_arg = node->children.at(1);
            auto maybe_set = second_arg->column;
            if (const auto * col_const = typeid_cast<const ColumnConst *>(maybe_set.get()))
                maybe_set = col_const->getDataColumnPtr();

            if (const auto * col_set = typeid_cast<const ColumnSet *>(maybe_set.get()))
            {
                auto future_set = col_set->getData();
                if (auto * set_from_subquery = typeid_cast<FutureSetFromSubquery *>(future_set.get());
                    set_from_subquery && set_from_subquery->getSourceAST())
                {
                    const auto temporary_table_name = fmt::format("_data_{}", toString(set_from_subquery->getHash()));

                    auto & external_table = (*external_tables)[temporary_table_name];
                    if (!external_table)
                    {
                        /// Here we create a new temporary table and attach it to the FutureSetFromSubquery.
                        /// At the moment FutureSet is created, temporary table will be filled.
                        /// This should happen because filter expression on initiator needs the set as well,
                        /// and it should be built before sending the external tables.

                        auto header = InterpreterSelectQueryAnalyzer::getSampleBlock(set_from_subquery->getSourceAST(), context);
                        NamesAndTypesList columns = header.getNamesAndTypesList();

                        auto external_storage_holder = TemporaryTableHolder(
                            context,
                            ColumnsDescription{columns},
                            ConstraintsDescription{},
                            nullptr /*query*/,
                            true /*create_for_global_subquery*/);

                        external_table = external_storage_holder.getTable();
                        set_from_subquery->setExternalTable(external_table);
                    }

                    node_to_ast[second_arg] = std::make_shared<ASTIdentifier>(temporary_table_name);
                }
            }
        }

        auto function = makeASTFunction(node->function_base->getName(), std::move(arguments));
        res = std::move(function);
    }

    return node_to_ast[dag.getOutputs().front()];
}

static void addFilters(
    Tables * external_tables,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    const ActionsDAG & pushed_down_filters)
{
    if (!query_tree || !planner_context)
        return;

    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::allow_push_predicate_ast_for_distributed_subqueries])
        return;

    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        return;

    /// We are building a set with projection names and a map with execution names here.
    /// They are needed to substitute inputs in ActionsDAG. See comment in tryBuildAdditionalFilterAST.

    std::unordered_set<std::string> projection_names;
    for (const auto & col : query_node->getProjectionColumns())
        projection_names.insert(col.name);

    std::unordered_map<std::string, QueryTreeNodePtr> execution_name_to_projection_query_tree;
    for (const auto & node : query_node->getProjection())
        execution_name_to_projection_query_tree[calculateActionNodeName(node, *planner_context)] = node;

    ASTPtr predicate = tryBuildAdditionalFilterAST(pushed_down_filters, projection_names, execution_name_to_projection_query_tree, external_tables, context);
    if (!predicate)
        return;

    auto table_expressions = extractTableExpressions(query_node->getJoinTree());
    /// Case with JOIN is not supported so far.
    if (table_expressions.size() != 1)
        return;

    const auto * table_node = table_expressions.front()->as<TableNode>();
    if (!table_node)
        return;

    TableWithColumnNamesAndTypes table_with_columns(
        DatabaseAndTableWithAlias(table_node->toASTIdentifier()),
        table_node->getStorageSnapshot()->getColumns(GetColumnsOptions::Kind::Ordinary));
    table_with_columns.table.alias = table_node->getAlias();

    bool optimize_final = settings[Setting::enable_optimize_predicate_expression_to_final_subquery];
    bool optimize_with = settings[Setting::allow_push_predicate_when_subquery_contains_with];

    ASTs predicates{predicate};
    PredicateRewriteVisitor::Data data(context, predicates, table_with_columns, optimize_final, optimize_with);

    data.rewriteSubquery(getSelectQuery(query_ast), table_with_columns.columns.getNames());
}

void ReadFromRemote::addLazyPipe(
    Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard, const Header & out_header, size_t parallel_marshalling_threads)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    const bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];

    if (stage == QueryProcessingStage::Complete)
    {
        if (const auto * ast_select = shard.query->as<ASTSelectQuery>())
            add_totals = ast_select->group_by_with_totals;
        add_extremes = context->getSettingsRef()[Setting::extremes];
    }

    std::shared_ptr<const ActionsDAG> pushed_down_filters = filter_actions_dag;

    const StorageID resolved_id = context->resolveStorageID(shard.main_table ? shard.main_table : main_table);
    const StoragePtr storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
    if (!storage)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Storage with id {} not found", resolved_id);
    }

    auto lazily_create_stream = [
            my_shard = shard, my_shard_count = shard_count, query = shard.query, header = shard.header,
            my_context = context, my_throttler = throttler,
            my_main_table = main_table, my_table_func_ptr = table_func_ptr,
            my_scalars = scalars, my_external_tables = external_tables,
            my_stage = stage, my_storage = storage,
            add_agg_info, add_totals, add_extremes, async_read, async_query_sending,
            query_tree = shard.query_tree, planner_context = shard.planner_context,
            pushed_down_filters, parallel_marshalling_threads]() mutable
        -> QueryPipelineBuilder
    {
        auto current_settings = my_context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings)
                            .getSaturated(current_settings[Setting::max_execution_time]);

        // In case reading from parallel replicas is allowed, lazy case is not triggered,
        // so in this case it's required to get only one connection from the pool
        std::vector<ConnectionPoolWithFailover::TryResult> try_results;
        try
        {
            if (my_table_func_ptr)
                try_results = my_shard.shard_info.pool->getManyForTableFunction(timeouts, current_settings, PoolMode::GET_ONE);
            else
                try_results = my_shard.shard_info.pool->getManyChecked(
                    timeouts, current_settings, PoolMode::GET_ONE,
                    my_shard.main_table ? my_shard.main_table.getQualifiedName() : my_main_table.getQualifiedName());
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"),
                    "Connections to remote replicas of local shard {} failed, will use stale local replica", my_shard.shard_info.shard_num);
            else
                throw;
        }

        UInt32 max_remote_delay = 0;
        for (const auto & try_result : try_results)
        {
            if (!try_result.is_up_to_date)
                max_remote_delay = std::max(try_result.delay, max_remote_delay);
        }

        bool use_delayed_remote_source = false;
        fiu_do_on(FailPoints::use_delayed_remote_source,
        {
            use_delayed_remote_source = true;
        });

        if (!use_delayed_remote_source)
        {
            const auto replicated_storage = std::dynamic_pointer_cast<StorageReplicatedMergeTree>(my_storage);
            if (!replicated_storage)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected lazy remote read from a non-replicated table: {}", my_storage->getName());
            }
            const UInt64 local_delay = replicated_storage->getAbsoluteDelay();
            const UInt64 max_allowed_delay = current_settings[Setting::max_replica_delay_for_distributed_queries];

            if (try_results.empty() && local_delay >= max_allowed_delay)
            {
                throw Exception(
                    ErrorCodes::ALL_REPLICAS_ARE_STALE,
                    "Failed to connect to other replicas and the local replica's delay is {} which is higher than max_replica_delay_for_distributed_queries",
                    local_delay
                );
            }

            if (try_results.empty() || (local_delay < max_remote_delay && local_delay < max_allowed_delay))
            {
                auto plan = createLocalPlan(
                    query, header, my_context, my_stage, my_shard.shard_info.shard_num, my_shard_count, my_shard.has_missing_objects);

                return std::move(*plan->buildQueryPipeline(QueryPlanOptimizationSettings(my_context), BuildQueryPipelineSettings(my_context)));
            }
        }

        std::vector<IConnectionPool::Entry> connections;
        connections.reserve(try_results.size());
        for (auto & try_result : try_results)
            connections.emplace_back(std::move(try_result.entry));

        /// For the lazy case we are ignoring external tables.
        /// This is because the set could be build before the lambda call,
        /// and the temporary table which we are about to send would be empty.
        /// So that GLOBAL IN would work as local IN in the pushed-down predicate.
        if (pushed_down_filters)
            addFilters(nullptr, my_context, query, query_tree, planner_context, *pushed_down_filters);
        String query_string = formattedAST(query);
        auto stage_to_use = my_shard.query_plan ? QueryProcessingStage::QueryPlan : my_stage;

        my_scalars["_shard_num"] = Block{
            {DataTypeUInt32().createColumnConst(1, my_shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            std::move(connections), query_string, header, my_context, my_throttler, my_scalars, my_external_tables, stage_to_use, my_shard.query_plan);

        auto pipe = createRemoteSourcePipe(
            remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending, parallel_marshalling_threads);
        QueryPipelineBuilder builder;
        builder.init(std::move(pipe));
        return builder;
    };

    pipes.emplace_back(createDelayedPipe(shard.header, lazily_create_stream, add_totals, add_extremes));
    addConvertingActions(pipes.back(), out_header, shard.has_missing_objects);
}

void ReadFromRemote::addPipe(
    Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard, const Header & out_header, size_t parallel_marshalling_threads)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];
    bool parallel_replicas_disabled = context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas] == 0;
    if (stage == QueryProcessingStage::Complete)
    {
        if (const auto * ast_select = shard.query->as<ASTSelectQuery>())
            add_totals = ast_select->group_by_with_totals;
        add_extremes = context->getSettingsRef()[Setting::extremes];
    }

    scalars["_shard_num"]
        = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};

    if (context->canUseTaskBasedParallelReplicas())
    {
        if (context->getSettingsRef()[Setting::cluster_for_parallel_replicas].changed)
        {
            const String cluster_for_parallel_replicas = context->getSettingsRef()[Setting::cluster_for_parallel_replicas];
            if (cluster_for_parallel_replicas != cluster_name)
                LOG_INFO(
                    log,
                    "cluster_for_parallel_replicas has been set for the query but has no effect: {}. Distributed table cluster is "
                    "used: {}",
                    cluster_for_parallel_replicas,
                    cluster_name);
        }

        LOG_TRACE(log, "Setting `cluster_for_parallel_replicas` to {}", cluster_name);
        context->setSetting("cluster_for_parallel_replicas", cluster_name);
    }

    /// parallel replicas custom key case
    if (shard.shard_filter_generator)
    {
        for (size_t i = 0; i < shard.shard_info.per_replica_pools.size(); ++i)
        {
            auto query = shard.query->clone();
            auto & select_query = getSelectQuery(query);
            auto shard_filter = shard.shard_filter_generator(i + 1);
            if (shard_filter)
            {
                auto where_expression = select_query.where();
                if (where_expression)
                    shard_filter = makeASTFunction("and", where_expression, shard_filter);

                select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(shard_filter));
            }

            const String query_string = formattedAST(query);

            if (!priority_func_factory.has_value())
                priority_func_factory = GetPriorityForLoadBalancing(LoadBalancing::ROUND_ROBIN, randomSeed());

            GetPriorityForLoadBalancing::Func priority_func
                = priority_func_factory->getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, shard.shard_info.pool->getPoolSize());

            auto stage_to_use = shard.query_plan ? QueryProcessingStage::QueryPlan : stage;

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                shard.shard_info.pool,
                query_string,
                shard.header,
                context,
                throttler,
                scalars,
                external_tables,
                stage_to_use,
                shard.query_plan,
                std::nullopt,
                priority_func);
            remote_query_executor->setLogger(log);
            remote_query_executor->setPoolMode(PoolMode::GET_ONE);

            if (!table_func_ptr)
                remote_query_executor->setMainTable(shard.main_table ? shard.main_table : main_table);

            pipes.emplace_back(
                createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending, parallel_marshalling_threads));
            addConvertingActions(pipes.back(), *output_header, shard.has_missing_objects);
        }
    }
    else
    {
        if (filter_actions_dag)
            addFilters(&external_tables, context, shard.query, shard.query_tree, shard.planner_context, *filter_actions_dag);

        const String query_string = formattedAST(shard.query);
        auto stage_to_use = shard.query_plan ? QueryProcessingStage::QueryPlan : stage;

        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard.shard_info.pool, query_string, shard.header, context, throttler, scalars, external_tables, stage_to_use, shard.query_plan);
        remote_query_executor->setLogger(log);

        if (context->canUseTaskBasedParallelReplicas() || parallel_replicas_disabled)
        {
            // when doing parallel reading from replicas (ParallelReplicasMode::READ_TASKS) on a shard:
            // establish a connection to a replica on the shard, the replica will instantiate coordinator to manage parallel reading from replicas on the shard.
            // The coordinator will return query result from the shard.
            // Only one coordinator per shard is necessary. Therefore using PoolMode::GET_ONE to establish only one connection per shard.
            // Using PoolMode::GET_MANY for this mode will(can) lead to instantiation of several coordinators (depends on max_parallel_replicas setting)
            // each will execute parallel reading from replicas, so the query result will be multiplied by the number of created coordinators
            //
            // In case parallel replicas are disabled, there also should be a single connection to each shard to prevent result duplication
            remote_query_executor->setPoolMode(PoolMode::GET_ONE);
        }
        else
            remote_query_executor->setPoolMode(PoolMode::GET_MANY);

        if (!table_func_ptr)
            remote_query_executor->setMainTable(shard.main_table ? shard.main_table : main_table);

        pipes.emplace_back(
            createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending, parallel_marshalling_threads));
        addConvertingActions(pipes.back(), out_header, shard.has_missing_objects);
    }
}

Pipes ReadFromRemote::addPipes(const ClusterProxy::SelectStreamFactory::Shards & used_shards, const Header & out_header)
{
    Pipes pipes;

    for (const auto & shard : used_shards)
    {
        const size_t parallel_marshalling_threads
            = (context->getSettingsRef()[Setting::max_threads] + used_shards.size() - 1) / used_shards.size();
        if (shard.lazy)
            addLazyPipe(pipes, shard, out_header, parallel_marshalling_threads);
        else
            addPipe(pipes, shard, out_header, parallel_marshalling_threads);
    }

    return pipes;
}

void ReadFromRemote::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes = addPipes(shards, *output_header);

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}

static ASTPtr makeExplain(const ExplainPlanOptions & options, ASTPtr query)
{
    auto explain_settings = std::make_shared<ASTSetQuery>();
    explain_settings->is_standalone = false;
    explain_settings->changes =  options.toSettingsChanges();

    auto explain_query = std::make_shared<ASTExplainQuery>(ASTExplainQuery::ExplainKind::QueryPlan);
    explain_query->setExplainedQuery(query);
    explain_query->setSettings(explain_settings);

    return explain_query;
}

static void formatExplain(IQueryPlanStep::FormatSettings & settings, Pipes pipes)
{
    String prefix(settings.offset + settings.indent, settings.indent_char);
    for (auto & pipe : pipes)
    {
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        Chunk chunk;
        while (executor.pull(chunk))
        {
            if (!chunk.hasColumns() || !chunk.hasRows())
                continue;

            const auto & col = chunk.getColumns().front();
            size_t num_rows = col->size();

            for (size_t row = 0; row < num_rows; ++row)
                settings.out << prefix << col->getDataAt(row).toView() << '\n';
        }
    }
}

void ReadFromRemote::describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options)
{
    Block header{ColumnWithTypeAndName{ColumnString::create(), std::make_shared<DataTypeString>(), "explain"}};
    ClusterProxy::SelectStreamFactory::Shards used_shards;

    for (const auto & shard : shards)
    {
        if (shard.query_plan)
        {
            shard.query_plan->explainPlan(settings.out, options, settings.offset / std::max<size_t>(settings.indent, 1) + 1);
        }
        else
        {
            auto & shard_copy = used_shards.emplace_back(shard);
            shard_copy.header = header;
            shard_copy.query = makeExplain(options, shard.query);
        }
    }

    formatExplain(settings, addPipes(used_shards, header));
}

bool ReadFromRemote::hasSerializedPlan() const
{
    for (const auto & shard : shards)
        if (shard.query_plan)
            return true;

    return false;
}


ReadFromParallelRemoteReplicasStep::ReadFromParallelRemoteReplicasStep(
    ASTPtr query_ast_,
    ClusterPtr cluster_,
    const StorageID & storage_id_,
    ParallelReplicasReadingCoordinatorPtr coordinator_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    ContextMutablePtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    LoggerPtr log_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    std::vector<ConnectionPoolPtr> pools_to_use_,
    std::optional<size_t> exclude_pool_index_,
    ConnectionPoolWithFailoverPtr connection_pool_with_failover_)
    : ISourceStep(std::move(header_))
    , cluster(cluster_)
    , query_ast(query_ast_)
    , storage_id(storage_id_)
    , coordinator(std::move(coordinator_))
    , stage(std::move(stage_))
    , context(context_)
    , throttler(throttler_)
    , scalars(scalars_)
    , external_tables{external_tables_}
    , storage_limits(std::move(storage_limits_))
    , log(log_)
    , pools_to_use(std::move(pools_to_use_))
    , exclude_pool_index(exclude_pool_index_)
    , connection_pool_with_failover(connection_pool_with_failover_)
{
    chassert(cluster->getShardCount() == 1);

    std::vector<String> replicas;
    replicas.reserve(pools_to_use.size());

    for (size_t i = 0, l = pools_to_use.size(); i < l; ++i)
    {
        if (exclude_pool_index.has_value() && i == exclude_pool_index)
            continue;

        replicas.push_back(pools_to_use[i]->getAddress());
    }

    auto description = fmt::format("Query: {} Replicas: {}", formattedAST(query_ast), fmt::join(replicas, ", "));
    setStepDescription(std::move(description));
}

void ReadFromParallelRemoteReplicasStep::enableMemoryBoundMerging()
{
    DB::enableMemoryBoundMerging(stage, nullptr, *context);
}

void ReadFromParallelRemoteReplicasStep::enforceAggregationInOrder(const SortDescription & sort_description)
{
    DB::enforceAggregationInOrder(stage, nullptr, sort_description, *context);
}

void ReadFromParallelRemoteReplicasStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes = addPipes(query_ast, *output_header);

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}

Pipes ReadFromParallelRemoteReplicasStep::addPipes(ASTPtr ast, const Header & out_header)
{
    Pipes pipes;

    std::vector<std::string_view> addresses;
    addresses.reserve(pools_to_use.size());
    for (size_t i = 0, l = pools_to_use.size(); i < l; ++i)
    {
        if (exclude_pool_index.has_value() && i == exclude_pool_index)
            continue;

        addresses.emplace_back(pools_to_use[i]->getAddress());
    }
    LOG_DEBUG(getLogger("ReadFromParallelRemoteReplicasStep"), "Addresses to use: {}", fmt::join(addresses, ", "));

    using ProcessorWeakPtr = std::weak_ptr<IProcessor>;
    std::unordered_map<size_t, ProcessorWeakPtr> remote_sources;
    for (size_t i = 0, l = pools_to_use.size(); i < l; ++i)
    {
        if (exclude_pool_index.has_value() && i == exclude_pool_index)
            continue;

        IConnections::ReplicaInfo replica_info{
            /// we should use this number specifically because efficiency of data distribution by consistent hash depends on it.
            .number_of_current_replica = i,
        };

        LOG_DEBUG(
            getLogger("ReadFromParallelRemoteReplicasStep"),
            "Replica number {} assigned to address {}",
            replica_info.number_of_current_replica,
            pools_to_use[i]->getAddress());

        const size_t parallel_marshalling_threads
            = (context->getSettingsRef()[Setting::max_threads] + pools_to_use.size() - 1) / pools_to_use.size();
        Pipe pipe = createPipeForSingeReplica(pools_to_use[i], ast, replica_info, out_header, parallel_marshalling_threads);
        remote_sources.emplace(replica_info.number_of_current_replica, pipe.getProcessors().front());
        pipes.emplace_back(std::move(pipe));
    }

    bool wait_for_unused_replicas = false;
    fiu_do_on(FailPoints::parallel_replicas_wait_for_unused_replicas,
    {
        wait_for_unused_replicas = true;
    });
    if (!wait_for_unused_replicas)
    {
        coordinator->setReadCompletedCallback(
            [sources = std::move(remote_sources)](const std::set<size_t> & used_replicas)
            {
                for (const auto & [replica_num, processor] : sources)
                {
                    if (used_replicas.contains(replica_num))
                        continue;

                    auto proc = processor.lock();
                    if (proc)
                        proc->cancel();
                }
            });
    }

    return pipes;
}

Pipe ReadFromParallelRemoteReplicasStep::createPipeForSingeReplica(
    const ConnectionPoolPtr & pool, ASTPtr ast, IConnections::ReplicaInfo replica_info, const Header & out_header,
    size_t parallel_marshalling_threads)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];

    String query_string = formattedAST(ast);

    if (ast->as<ASTExplainQuery>() == nullptr)
        assert(stage != QueryProcessingStage::Complete);

    assert(output_header);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        pool,
        query_string,
        out_header,
        context,
        throttler,
        scalars,
        external_tables,
        stage,
        RemoteQueryExecutor::Extension{.parallel_reading_coordinator = coordinator, .replica_info = std::move(replica_info)},
        connection_pool_with_failover);

    remote_query_executor->setLogger(log);
    remote_query_executor->setMainTable(storage_id);

    Pipe pipe
        = createRemoteSourcePipe(std::move(remote_query_executor), add_agg_info, add_totals, add_extremes, async_read, async_query_sending, parallel_marshalling_threads);
    addConvertingActions(pipe, out_header);
    return pipe;
}

void ReadFromParallelRemoteReplicasStep::describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options)
{
    Block header{ColumnWithTypeAndName{ColumnString::create(), std::make_shared<DataTypeString>(), "explain"}};

    auto explain_query = makeExplain(options, query_ast);
    formatExplain(settings, addPipes(explain_query, header));
}

}
