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
#include <Interpreters/ActionsDAG.h>
#include "Common/logger_useful.h"
#include <Common/checkStackSize.h>
#include <Core/QueryProcessingStage.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int LOGICAL_ERROR;
}

static void addConvertingActions(Pipe & pipe, const Block & header)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    auto get_converting_dag = [](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);
    };

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
    Poco::Logger * log_,
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
    bool async_read = context->getSettingsRef().async_socket_for_remote;
    const bool async_query_sending = context->getSettingsRef().async_query_sending_for_remote;

    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
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
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
            current_settings).getSaturated(
                current_settings.max_execution_time);
        std::vector<ConnectionPoolWithFailover::TryResult> try_results;
        try
        {
            if (my_table_func_ptr)
                try_results = my_shard.shard_info.pool->getManyForTableFunction(timeouts, &current_settings, PoolMode::GET_MANY);
            else
                try_results = my_shard.shard_info.pool->getManyChecked(
                    timeouts, &current_settings, PoolMode::GET_MANY,
                    my_shard.main_table ? my_shard.main_table.getQualifiedName() : my_main_table.getQualifiedName());
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "Connections to remote replicas of local shard {} failed, will use stale local replica", my_shard.shard_info.shard_num);
            else
                throw;
        }

        double max_remote_delay = 0.0;
        for (const auto & try_result : try_results)
        {
            if (!try_result.is_up_to_date)
                max_remote_delay = std::max(try_result.staleness, max_remote_delay);
        }

        if (try_results.empty() || local_delay < max_remote_delay)
        {
            auto plan = createLocalPlan(
                query, header, my_context, my_stage, my_shard.shard_info.shard_num, my_shard_count, 0, 0, /*coordinator=*/nullptr);

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
    addConvertingActions(pipes.back(), output_stream->header);
}

void ReadFromRemote::addPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef().async_socket_for_remote;
    bool async_query_sending = context->getSettingsRef().async_query_sending_for_remote;
    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    String query_string = formattedAST(shard.query);

    scalars["_shard_num"]
        = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};

    if (context->getParallelReplicasMode() == Context::ParallelReplicasMode::READ_TASKS)
    {
        if (context->getSettingsRef().cluster_for_parallel_replicas.changed)
        {
            const String cluster_for_parallel_replicas = context->getSettingsRef().cluster_for_parallel_replicas;
            if (cluster_for_parallel_replicas != cluster_name)
                LOG_INFO(log, "cluster_for_parallel_replicas has been set for the query but has no effect: {}. Distributed table cluster is used: {}",
                         cluster_for_parallel_replicas, cluster_name);
        }
        context->setSetting("cluster_for_parallel_replicas", cluster_name);
    }

    std::shared_ptr<RemoteQueryExecutor> remote_query_executor;

    remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard.shard_info.pool, query_string, output_stream->header, context, throttler, scalars, external_tables, stage);

    remote_query_executor->setLogger(log);

    if (context->getParallelReplicasMode() == Context::ParallelReplicasMode::READ_TASKS)
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

    pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read, async_query_sending));
    addConvertingActions(pipes.back(), output_stream->header);
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
    ParallelReplicasReadingCoordinatorPtr coordinator_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ContextMutablePtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    Poco::Logger * log_,
    std::shared_ptr<const StorageLimitsList> storage_limits_)
    : ISourceStep(DataStream{.header = std::move(header_)})
    , cluster(cluster_)
    , query_ast(query_ast_)
    , coordinator(std::move(coordinator_))
    , stage(std::move(stage_))
    , main_table(std::move(main_table_))
    , table_func_ptr(table_func_ptr_)
    , context(context_)
    , throttler(throttler_)
    , scalars(scalars_)
    , external_tables{external_tables_}
    , storage_limits(std::move(storage_limits_))
    , log(log_)
{
    std::vector<String> description;

    for (const auto & address : cluster->getShardsAddresses())
        description.push_back(fmt::format("Replica: {}", address[0].host_name));

    setStepDescription(boost::algorithm::join(description, ", "));
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
    const Settings & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

    size_t all_replicas_count = current_settings.max_parallel_replicas;
    if (all_replicas_count > cluster->getShardsInfo().size())
    {
        LOG_INFO(&Poco::Logger::get("ReadFromParallelRemoteReplicasStep"),
            "The number of replicas requested ({}) is bigger than the real number available in the cluster ({}). "\
            "Will use the latter number to execute the query.", current_settings.max_parallel_replicas, cluster->getShardsInfo().size());
        all_replicas_count = cluster->getShardsInfo().size();
    }

    /// Find local shard. It might happen that there is no local shard, but that's fine
    for (const auto & shard: cluster->getShardsInfo())
    {
        if (shard.isLocal())
        {
            IConnections::ReplicaInfo replica_info
            {
                .all_replicas_count = all_replicas_count,
                .number_of_current_replica = 0
            };

            addPipeForSingeReplica(pipes, shard.pool, replica_info);
        }
    }

    auto current_shard = cluster->getShardsInfo().begin();
    while (pipes.size() != all_replicas_count)
    {
        if (current_shard->isLocal())
        {
            ++current_shard;
            continue;
        }

        IConnections::ReplicaInfo replica_info
        {
            .all_replicas_count = all_replicas_count,
            .number_of_current_replica = pipes.size()
        };

        addPipeForSingeReplica(pipes, current_shard->pool, replica_info);
        ++current_shard;
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    pipeline.init(std::move(pipe));

}


void ReadFromParallelRemoteReplicasStep::addPipeForSingeReplica(Pipes & pipes, std::shared_ptr<ConnectionPoolWithFailover> pool, IConnections::ReplicaInfo replica_info)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef().async_socket_for_remote;
    bool async_query_sending = context->getSettingsRef().async_query_sending_for_remote;

    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = query_ast->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    String query_string = formattedAST(query_ast);

    assert(stage != QueryProcessingStage::Complete);
    assert(output_stream);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        pool, query_string, output_stream->header, context, throttler, scalars, external_tables, stage,
        RemoteQueryExecutor::Extension{.parallel_reading_coordinator = coordinator, .replica_info = std::move(replica_info)});

    remote_query_executor->setLogger(log);

    pipes.emplace_back(createRemoteSourcePipe(std::move(remote_query_executor), add_agg_info, add_totals, add_extremes, async_read, async_query_sending));
    addConvertingActions(pipes.back(), output_stream->header);
}

}
