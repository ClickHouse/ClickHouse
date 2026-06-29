#include <Processors/QueryPlan/ReadFromParallelReplicas.h>
#include <DataTypes/DataTypeString.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
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
#include <Common/FailPoint.h>
#include "Planner/Utils.h"
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

#include <fmt/format.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
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
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool parallel_replicas_filter_pushdown;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char use_delayed_remote_source[];
    extern const char parallel_replicas_wait_for_unused_replicas[];
}

static void addConvertingActions(Pipe & pipe, const Block & header, const ContextPtr & context, bool use_positions_to_match = false)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    auto match_mode = use_positions_to_match ? ActionsDAG::MatchColumnsMode::Position : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode = match_mode, context](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
            context,
            true);
    };

    if (use_positions_to_match)
        pipe.addSimpleTransform([](const SharedHeader & stream_header) { return std::make_shared<MaterializingTransform>(stream_header); });

    auto convert_actions = std::make_shared<ExpressionActions>(get_converting_dag(pipe.getHeader(), header));
    pipe.addSimpleTransform([&](const SharedHeader & cur_header, Pipe::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(cur_header, convert_actions);
    });
}

ReadFromParallelReplicasStep::ReadFromParallelReplicasStep(
    ClusterPtr cluster_,
    const StorageID & storage_id_,
    ParallelReplicasReadingCoordinatorPtr coordinator_,
    SharedHeader header_,
    ContextMutablePtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    LoggerPtr log_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    std::vector<ConnectionPoolPtr> pools_to_use_,
    std::optional<size_t> exclude_pool_index_,
    ConnectionPoolWithFailoverPtr connection_pool_with_failover_,
    std::shared_ptr<const QueryPlan> query_plan_)
    : SourceStepWithFilterBase(std::move(header_))
    , cluster(cluster_)
    , storage_id(storage_id_)
    , coordinator(std::move(coordinator_))
    , context(context_)
    , throttler(throttler_)
    , scalars(scalars_)
    , external_tables{external_tables_}
    , storage_limits(std::move(storage_limits_))
    , log(log_)
    , pools_to_use(std::move(pools_to_use_))
    , exclude_pool_index(exclude_pool_index_)
    , connection_pool_with_failover(connection_pool_with_failover_)
    , query_plan(std::move(query_plan_))
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

    auto description = fmt::format("QueryPlan: {} Replicas: {}", dumpQueryPlan(*query_plan), fmt::join(replicas, ", "));
    setStepDescription(std::move(description), context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
}

void ReadFromParallelReplicasStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes = addPipes(output_header);

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}

Pipes ReadFromParallelReplicasStep::addPipes(const SharedHeader & out_header)
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
    LOG_DEBUG(getLogger("ReadFromParallelReplicas"), "Addresses to use: {}", fmt::join(addresses, ", "));

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
            getLogger("ReadFromParallelReplicas"),
            "Replica number {} assigned to address {}",
            replica_info.number_of_current_replica,
            pools_to_use[i]->getAddress());

        const size_t parallel_marshalling_threads
            = (context->getSettingsRef()[Setting::max_threads] + pools_to_use.size() - 1) / pools_to_use.size();
        Pipe pipe = createPipeForSingeReplica(pools_to_use[i], replica_info, out_header, parallel_marshalling_threads);
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

Pipe ReadFromParallelReplicasStep::createPipeForSingeReplica(
    const ConnectionPoolPtr & pool,
    IConnections::ReplicaInfo replica_info,
    const SharedHeader & out_header,
    size_t parallel_marshalling_threads)
{
    const bool add_agg_info = false;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];

    chassert(output_header);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        pool,
        dumpQueryPlan(*query_plan),
        out_header,
        context,
        throttler,
        scalars,
        external_tables,
        QueryProcessingStage::QueryPlan,
        RemoteQueryExecutor::Extension{.parallel_reading_coordinator = coordinator, .replica_info = std::move(replica_info)},
        connection_pool_with_failover,
        query_plan);

    remote_query_executor->setLogger(log);
    remote_query_executor->setMainTable(storage_id);
    remote_query_executor->setDistributedFanout(pools_to_use.size() - (exclude_pool_index.has_value() ? 1 : 0));

    Pipe pipe = createRemoteSourcePipe(
        std::move(remote_query_executor),
        add_agg_info,
        add_totals,
        add_extremes,
        async_read,
        async_query_sending,
        parallel_marshalling_threads);
    addConvertingActions(pipe, *out_header, context);
    return pipe;
}

void ReadFromParallelReplicasStep::describeDistributedPlan(FormatSettings & settings, const ExplainPlanOptions & options)
{
    query_plan->explainPlan(settings.out, options, settings.offset / std::max<size_t>(settings.base_indent, 1) + 1);
}

void ReadFromParallelReplicasStep::describeDistributedPipeline(FormatSettings &, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXPLAIN PIPELINE distributed is not supported");
}

}
