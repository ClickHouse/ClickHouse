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
    ClusterProxy::IStreamFactory::Shards shards_,
    Block header_,
    QueryProcessingStage::Enum stage_,
    StorageID main_table_,
    ASTPtr table_func_ptr_,
    ContextPtr context_,
    ThrottlerPtr throttler_,
    Scalars scalars_,
    Tables external_tables_,
    Poco::Logger * log_,
    UInt32 shard_count_)
    : ISourceStep(DataStream{.header = std::move(header_)})
    , shards(std::move(shards_))
    , stage(stage_)
    , main_table(std::move(main_table_))
    , table_func_ptr(std::move(table_func_ptr_))
    , context(std::move(context_))
    , throttler(std::move(throttler_))
    , scalars(std::move(scalars_))
    , external_tables(std::move(external_tables_))
    , log(log_)
    , shard_count(shard_count_)
{
}

void ReadFromRemote::addLazyPipe(Pipes & pipes, const ClusterProxy::IStreamFactory::Shard & shard,
    std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator,
    std::shared_ptr<ConnectionPoolWithFailover> pool,
    std::optional<IConnections::ReplicaInfo> replica_info)
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
            replica_info = replica_info,
            pool = pool ? pool : shard.pool,
            coordinator = coordinator,
            shard_num = shard.shard_num, shard_count = shard_count, query = shard.query, header = shard.header,
            context = context, throttler = throttler,
            main_table = main_table, table_func_ptr = table_func_ptr,
            scalars = scalars, external_tables = external_tables,
            stage = stage, local_delay = shard.local_delay,
            add_agg_info, add_totals, add_extremes, async_read]() mutable
        -> Pipe
    {
        auto current_settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
            current_settings).getSaturated(
                current_settings.max_execution_time);
        std::vector<ConnectionPoolWithFailover::TryResult> try_results;
        try
        {
            if (table_func_ptr)
                try_results = pool->getManyForTableFunction(timeouts, &current_settings, PoolMode::GET_MANY);
            else
                try_results = pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, main_table.getQualifiedName());
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "Connections to remote replicas of local shard {} failed, will use stale local replica", shard_num);
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
            auto plan = createLocalPlan(query, header, context, stage, shard_num, shard_count, coordinator);

            return QueryPipelineBuilder::getPipe(std::move(*plan->buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(context),
                BuildQueryPipelineSettings::fromContext(context))));
        }
        else
        {
            std::vector<IConnectionPool::Entry> connections;
            connections.reserve(try_results.size());
            for (auto & try_result : try_results)
                connections.emplace_back(std::move(try_result.entry));

            String query_string = formattedAST(query);

            scalars["_shard_num"]
                = Block{{DataTypeUInt32().createColumnConst(1, shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                pool, std::move(connections), query_string, header, context, throttler, scalars, external_tables, stage,
                RemoteQueryExecutor::Extension{.parallel_reading_coordinator = std::move(coordinator), .replica_info = replica_info});

            return createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read);
        }
    };

    pipes.emplace_back(createDelayedPipe(shard.header, lazily_create_stream, add_totals, add_extremes));
    pipes.back().addInterpreterContext(context);
    addConvertingActions(pipes.back(), output_stream->header);
}

void ReadFromRemote::addPipe(Pipes & pipes, const ClusterProxy::IStreamFactory::Shard & shard,
    std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator,
    std::shared_ptr<ConnectionPoolWithFailover> pool,
    std::optional<IConnections::ReplicaInfo> replica_info)
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
        = Block{{DataTypeUInt32().createColumnConst(1, shard.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}};

    std::shared_ptr<RemoteQueryExecutor> remote_query_executor;

    remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            pool ? pool : shard.pool, query_string, shard.header, context, throttler, scalars, external_tables, stage,
            RemoteQueryExecutor::Extension{.parallel_reading_coordinator = std::move(coordinator), .replica_info = std::move(replica_info)});

    remote_query_executor->setLogger(log);

    /// In case of parallel reading from replicas we have a connection pool per replica.
    /// Setting PoolMode will make no sense.
    if (!pool)
        remote_query_executor->setPoolMode(PoolMode::GET_MANY);

    if (!table_func_ptr)
        remote_query_executor->setMainTable(main_table);

    pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
    pipes.back().addInterpreterContext(context);
    addConvertingActions(pipes.back(), output_stream->header);
}

void ReadFromRemote::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;

    const auto & settings = context->getSettingsRef();
    const bool enable_sample_offset_parallel_processing = settings.max_parallel_replicas > 1 && !settings.allow_experimental_parallel_reading_from_replicas;

    /// We have to create a pipe for each replica
    /// FIXME: The second condition is only for tests to work, because hedged connections enabled by default.
    if (settings.max_parallel_replicas > 1 && !enable_sample_offset_parallel_processing && !context->getSettingsRef().use_hedged_requests)
    {
        const Settings & current_settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

        for (const auto & shard : shards)
        {
            auto coordinator = std::make_shared<ParallelReplicasReadingCoordinator>();

            for (size_t replica_num = 0; replica_num < shard.num_replicas; ++replica_num)
            {
                IConnections::ReplicaInfo replica_info
                {
                    .all_replicas_count = shard.num_replicas,
                    .number_of_current_replica = replica_num
                };

                auto pool = shard.per_replica_pools[replica_num];
                auto pool_with_failover =  std::make_shared<ConnectionPoolWithFailover>(
                    ConnectionPoolPtrs{pool}, current_settings.load_balancing);

                if (shard.lazy)
                    addLazyPipe(pipes, shard, coordinator, pool_with_failover, replica_info);
                else
                    addPipe(pipes, shard, coordinator, pool_with_failover, replica_info);
            }
        }
    }
    else
    {
        for (const auto & shard : shards)
        {
            if (shard.lazy)
                addLazyPipe(pipes, shard, /*coordinator=*/nullptr, /*pool*/{}, /*replica_info*/std::nullopt);
            else
                addPipe(pipes, shard, /*coordinator=*/nullptr, /*pool*/{}, /*replica_info*/std::nullopt);
        }
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    pipeline.init(std::move(pipe));
}

}
