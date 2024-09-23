#include <Processors/QueryPlan/ReadFromRemote.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ASTFunction.h>

#include <fmt/format.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool extremes;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 parallel_replicas_mark_segment_size;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int LOGICAL_ERROR;
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

static void enforceSorting(QueryProcessingStage::Enum stage, DataStream & output_stream, Context & context, SortDescription output_sort_description)
{
    if (stage != QueryProcessingStage::WithMergeableState)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot enforce sorting for ReadFromRemote step up to stage {}",
            QueryProcessingStage::toString(stage));

    context.setSetting("enable_memory_bound_merging_of_aggregation_results", true);

    output_stream.sort_description = std::move(output_sort_description);
    output_stream.sort_scope = DataStream::SortScope::Stream;
}

static void enforceAggregationInOrder(QueryProcessingStage::Enum stage, Context & context)
{
    if (stage != QueryProcessingStage::WithMergeableState)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot enforce aggregation in order for ReadFromRemote step up to stage {}",
            QueryProcessingStage::toString(stage));

    context.setSetting("optimize_aggregation_in_order", true);
    context.setSetting("force_aggregation_in_order", true);
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true, /*hilite*/ false, /*always_quote_identifiers*/ true);
    ast->format(ast_format_settings);
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
    : ISourceStep(DataStream{.header = std::move(header_)})
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

void ReadFromRemote::enforceSorting(SortDescription output_sort_description)
{
    DB::enforceSorting(stage, *output_stream, *context, output_sort_description);
}

void ReadFromRemote::enforceAggregationInOrder()
{
    DB::enforceAggregationInOrder(stage, *context);
}

void ReadFromRemote::addLazyPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    const bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];

    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef()[Setting::extremes];
    }

    auto lazily_create_stream = [
            my_shard = shard, my_shard_count = shard_count, query = shard.query, header = shard.header,
            my_context = context, my_throttler = throttler,
            my_main_table = main_table, my_table_func_ptr = table_func_ptr,
            my_scalars = scalars, my_external_tables = external_tables,
            my_stage = stage, local_delay = shard.local_delay,
            add_agg_info, add_totals, add_extremes, async_read, async_query_sending]() mutable
        -> QueryPipelineBuilder
    {
        auto current_settings = my_context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings)
                            .getSaturated(current_settings[Setting::max_execution_time]);

        std::vector<ConnectionPoolWithFailover::TryResult> try_results;
        try
        {
            if (my_table_func_ptr)
                try_results = my_shard.shard_info.pool->getManyForTableFunction(timeouts, current_settings, PoolMode::GET_MANY);
            else
                try_results = my_shard.shard_info.pool->getManyChecked(
                    timeouts, current_settings, PoolMode::GET_MANY,
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

        if (try_results.empty() || local_delay < max_remote_delay)
        {
            auto plan = createLocalPlan(
                query, header, my_context, my_stage, my_shard.shard_info.shard_num, my_shard_count, my_shard.has_missing_objects);

            return std::move(*plan->buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(my_context),
                BuildQueryPipelineSettings::fromContext(my_context)));
        }
        else
        {
            std::vector<IConnectionPool::Entry> connections;
            connections.reserve(try_results.size());
            for (auto & try_result : try_results)
                connections.emplace_back(std::move(try_result.entry));

            String query_string = formattedAST(query);

            my_scalars["_shard_num"]
                = Block{{DataTypeUInt32().createColumnConst(1, my_shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                std::move(connections), query_string, header, my_context, my_throttler, my_scalars, my_external_tables, my_stage);

            auto pipe = createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending);
            QueryPipelineBuilder builder;
            builder.init(std::move(pipe));
            return builder;
        }
    };

    pipes.emplace_back(createDelayedPipe(shard.header, lazily_create_stream, add_totals, add_extremes));
    addConvertingActions(pipes.back(), output_stream->header, shard.has_missing_objects);
}

void ReadFromRemote::addPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];
    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
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
            auto & select_query = query->as<ASTSelectQuery &>();
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

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                shard.shard_info.pool,
                query_string,
                shard.header,
                context,
                throttler,
                scalars,
                external_tables,
                stage,
                std::nullopt,
                priority_func);
            remote_query_executor->setLogger(log);
            remote_query_executor->setPoolMode(PoolMode::GET_ONE);

            if (!table_func_ptr)
                remote_query_executor->setMainTable(shard.main_table ? shard.main_table : main_table);

            pipes.emplace_back(
                createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending));
            addConvertingActions(pipes.back(), output_stream->header, shard.has_missing_objects);
        }
    }
    else
    {
        const String query_string = formattedAST(shard.query);

        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard.shard_info.pool, query_string, shard.header, context, throttler, scalars, external_tables, stage);
        remote_query_executor->setLogger(log);

        if (context->canUseTaskBasedParallelReplicas())
        {
            // when doing parallel reading from replicas (ParallelReplicasMode::READ_TASKS) on a shard:
            // establish a connection to a replica on the shard, the replica will instantiate coordinator to manage parallel reading from replicas on the shard.
            // The coordinator will return query result from the shard.
            // Only one coordinator per shard is necessary. Therefore using PoolMode::GET_ONE to establish only one connection per shard.
            // Using PoolMode::GET_MANY for this mode will(can) lead to instantiation of several coordinators (depends on max_parallel_replicas setting)
            // each will execute parallel reading from replicas, so the query result will be multiplied by the number of created coordinators
            remote_query_executor->setPoolMode(PoolMode::GET_ONE);
        }
        else
            remote_query_executor->setPoolMode(PoolMode::GET_MANY);

        if (!table_func_ptr)
            remote_query_executor->setMainTable(shard.main_table ? shard.main_table : main_table);

        pipes.emplace_back(
            createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending));
        addConvertingActions(pipes.back(), output_stream->header, shard.has_missing_objects);
    }
}

void ReadFromRemote::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;

    for (const auto & shard : shards)
    {
        if (shard.lazy)
            addLazyPipe(pipes, shard);
        else
            addPipe(pipes, shard);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
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
    std::optional<size_t> exclude_pool_index_)
    : ISourceStep(DataStream{.header = std::move(header_)})
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

void ReadFromParallelRemoteReplicasStep::enforceSorting(SortDescription output_sort_description)
{
    DB::enforceSorting(stage, *output_stream, *context, output_sort_description);
}

void ReadFromParallelRemoteReplicasStep::enforceAggregationInOrder()
{
    DB::enforceAggregationInOrder(stage, *context);
}

void ReadFromParallelRemoteReplicasStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
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

    for (size_t i = 0, l = pools_to_use.size(); i < l; ++i)
    {
        if (exclude_pool_index.has_value() && i == exclude_pool_index)
            continue;

        IConnections::ReplicaInfo replica_info{
            /// we should use this number specifically because efficiency of data distribution by consistent hash depends on it.
            .number_of_current_replica = i,
        };

        addPipeForSingeReplica(pipes, pools_to_use[i], replica_info);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));
}


void ReadFromParallelRemoteReplicasStep::addPipeForSingeReplica(
    Pipes & pipes, const ConnectionPoolPtr & pool, IConnections::ReplicaInfo replica_info)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef()[Setting::async_socket_for_remote];
    bool async_query_sending = context->getSettingsRef()[Setting::async_query_sending_for_remote];

    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = query_ast->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef()[Setting::extremes];
    }

    String query_string = formattedAST(query_ast);

    assert(stage != QueryProcessingStage::Complete);
    assert(output_stream);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        pool,
        query_string,
        output_stream->header,
        context,
        throttler,
        scalars,
        external_tables,
        stage,
        RemoteQueryExecutor::Extension{.parallel_reading_coordinator = coordinator, .replica_info = std::move(replica_info)});

    remote_query_executor->setLogger(log);
    remote_query_executor->setMainTable(storage_id);

    pipes.emplace_back(createRemoteSourcePipe(std::move(remote_query_executor), add_agg_info, add_totals, add_extremes, async_read, async_query_sending));
    addConvertingActions(pipes.back(), output_stream->header);
}

}
