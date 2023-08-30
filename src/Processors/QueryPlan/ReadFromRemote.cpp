#include <Processors/QueryPlan/ReadFromRemote.h>
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
#include <Interpreters/InterpreterSelectQuery.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Common/checkStackSize.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
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

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true);
    ast_format_settings.hilite = false;
    ast_format_settings.always_quote_identifiers = true;
    ast->format(ast_format_settings);
    return buf.str();
}

ReadFromRemote::ReadFromRemote(
    ClusterProxy::SelectStreamFactory::Shards shards_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ContextPtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    Poco::Logger * log_,
    UInt32 shard_count_,
    std::shared_ptr<const StorageLimitsList> storage_limits_)
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
{
}

void ReadFromRemote::addLazyPipe(Pipes & pipes, const ClusterProxy::SelectStreamFactory::Shard & shard)
{
    bool add_agg_info = stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef().async_socket_for_remote;
    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    auto lazily_create_stream = [
            shard = shard, shard_count = shard_count, query = shard.query, header = shard.header,
            context = context, throttler = throttler,
            main_table = main_table, table_func_ptr = table_func_ptr,
            scalars = scalars, external_tables = external_tables,
            stage = stage, local_delay = shard.local_delay,
            add_agg_info, add_totals, add_extremes, async_read]() mutable
        -> QueryPipelineBuilder
    {
        auto current_settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
            current_settings).getSaturated(
                current_settings.max_execution_time);
        std::vector<ConnectionPoolWithFailover::TryResult> try_results;
        try
        {
            if (table_func_ptr)
                try_results = shard.shard_info.pool->getManyForTableFunction(timeouts, &current_settings, PoolMode::GET_MANY);
            else
                try_results = shard.shard_info.pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, main_table.getQualifiedName());
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "Connections to remote replicas of local shard {} failed, will use stale local replica", shard.shard_info.shard_num);
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
            auto plan = createLocalPlan(query, header, context, stage, shard.shard_info.shard_num, shard_count, 0, 0, /*coordinator=*/nullptr);

            return std::move(*plan->buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(context),
                BuildQueryPipelineSettings::fromContext(context)));
        }
        else
        {
            std::vector<IConnectionPool::Entry> connections;
            connections.reserve(try_results.size());
            for (auto & try_result : try_results)
                connections.emplace_back(std::move(try_result.entry));

            String query_string = formattedAST(query);

            scalars["_shard_num"]
                = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                shard.shard_info.pool, std::move(connections), query_string, header, context, throttler, scalars, external_tables, stage);

            auto pipe = createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read);
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
    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    String query_string = formattedAST(shard.query);

    scalars["_shard_num"]
        = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};

    std::shared_ptr<RemoteQueryExecutor> remote_query_executor;

    remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard.shard_info.pool, query_string, shard.header, context, throttler, scalars, external_tables, stage);

    remote_query_executor->setLogger(log);
    remote_query_executor->setPoolMode(PoolMode::GET_MANY);

    if (!table_func_ptr)
        remote_query_executor->setMainTable(main_table);

    pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
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
    ParallelReplicasReadingCoordinatorPtr coordinator_,
    ClusterProxy::SelectStreamFactory::Shard shard_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ContextPtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    Poco::Logger * log_,
    std::shared_ptr<const StorageLimitsList> storage_limits_)
    : ISourceStep(DataStream{.header = std::move(header_)})
    , coordinator(std::move(coordinator_))
    , shard(std::move(shard_))
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

    for (const auto & address : shard.shard_info.all_addresses)
        if (!address.is_local)
            description.push_back(fmt::format("Replica: {}", address.host_name));

    setStepDescription(boost::algorithm::join(description, ", "));
}


void ReadFromParallelRemoteReplicasStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;

    const Settings & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

    for (size_t replica_num = 0; replica_num < shard.shard_info.getAllNodeCount(); ++replica_num)
    {
        if (shard.shard_info.all_addresses[replica_num].is_local)
            continue;

        IConnections::ReplicaInfo replica_info
        {
            .all_replicas_count = shard.shard_info.getAllNodeCount(),
            .number_of_current_replica = replica_num
        };

        auto pool = shard.shard_info.per_replica_pools[replica_num];
        assert(pool);

        auto pool_with_failover =  std::make_shared<ConnectionPoolWithFailover>(
            ConnectionPoolPtrs{pool}, current_settings.load_balancing);

        addPipeForSingeReplica(pipes, pool_with_failover, replica_info);
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
    if (stage == QueryProcessingStage::Complete)
    {
        add_totals = shard.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    String query_string = formattedAST(shard.query);

    scalars["_shard_num"]
        = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_info.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};

    std::shared_ptr<RemoteQueryExecutor> remote_query_executor;

    remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            pool, query_string, shard.header, context, throttler, scalars, external_tables, stage,
            RemoteQueryExecutor::Extension{.parallel_reading_coordinator = coordinator, .replica_info = std::move(replica_info)});

    remote_query_executor->setLogger(log);

    if (!table_func_ptr)
        remote_query_executor->setMainTable(main_table);

    pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
    addConvertingActions(pipes.back(), output_stream->header);
}

}
