#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/checkStackSize.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>

#include <common/logger_useful.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
    const Block & header_,
    QueryProcessingStage::Enum processed_stage_,
    bool has_virtual_shard_num_column_)
    : header(header_),
    processed_stage{processed_stage_},
    has_virtual_shard_num_column(has_virtual_shard_num_column_)
{
}


namespace
{

/// Special support for the case when `_shard_num` column is used in GROUP BY key expression.
/// This column is a constant for shard.
/// Constant expression with this column may be removed from intermediate header.
/// However, this column is not constant for initiator, and it expect intermediate header has it.
///
/// To fix it, the following trick is applied.
/// We check all GROUP BY keys which depend only on `_shard_num`.
/// Calculate such expression for current shard if it is used in header.
/// Those columns will be added to modified header as already known constants.
///
/// For local shard, missed constants will be added by converting actions.
/// For remote shard, RemoteQueryExecutor will automatically add missing constant.
Block evaluateConstantGroupByKeysWithShardNumber(
    const ContextPtr & context, const ASTPtr & query_ast, const Block & header, UInt32 shard_num)
{
    Block res;

    ColumnWithTypeAndName shard_num_col;
    shard_num_col.type = std::make_shared<DataTypeUInt32>();
    shard_num_col.column = shard_num_col.type->createColumnConst(0, shard_num);
    shard_num_col.name = "_shard_num";

    if (auto group_by = query_ast->as<ASTSelectQuery &>().groupBy())
    {
        for (const auto & elem : group_by->children)
        {
            String key_name = elem->getColumnName();
            if (header.has(key_name))
            {
                auto ast = elem->clone();

                RequiredSourceColumnsVisitor::Data columns_context;
                RequiredSourceColumnsVisitor(columns_context).visit(ast);

                auto required_columns = columns_context.requiredColumns();
                if (required_columns.size() != 1 || required_columns.count("_shard_num") == 0)
                    continue;

                Block block({shard_num_col});
                auto syntax_result = TreeRewriter(context).analyze(ast, {NameAndTypePair{shard_num_col.name, shard_num_col.type}});
                ExpressionAnalyzer(ast, syntax_result, context).getActions(true, false)->execute(block);

                res.insert(block.getByName(key_name));
            }
        }
    }

    /// We always add _shard_num constant just in case.
    /// For initial query it is considered as a column from table, and may be required by intermediate block.
    if (!res.has(shard_num_col.name))
        res.insert(std::move(shard_num_col));

    return res;
}

ActionsDAGPtr getConvertingDAG(const Block & block, const Block & header)
{
    /// Convert header structure to expected.
    /// Also we ignore constants from result and replace it with constants from header.
    /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
    return ActionsDAG::makeConvertingActions(
        block.getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        true);
}

void addConvertingActions(QueryPlan & plan, const Block & header)
{
    if (blocksHaveEqualStructure(plan.getCurrentDataStream().header, header))
        return;

    auto convert_actions_dag = getConvertingDAG(plan.getCurrentDataStream().header, header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), convert_actions_dag);
    plan.addStep(std::move(converting));
}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();

    InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions(processed_stage));
    interpreter.buildQueryPlan(*query_plan);

    addConvertingActions(*query_plan, header);

    return query_plan;
}

}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards)
{
    auto modified_query_ast = query_ast->clone();
    auto modified_header = header;
    if (has_virtual_shard_num_column)
    {
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_shard_num", shard_info.shard_num, "toUInt32");
        auto shard_num_constants = evaluateConstantGroupByKeysWithShardNumber(context, query_ast, modified_header, shard_info.shard_num);

        for (auto & col : shard_num_constants)
        {
            if (modified_header.has(col.name))
                modified_header.getByName(col.name).column = std::move(col.column);
            else
                modified_header.insert(std::move(col));
        }
    }

    auto emplace_local_stream = [&]()
    {
        local_plans.emplace_back(createLocalPlan(modified_query_ast, modified_header, context, processed_stage));
        addConvertingActions(*local_plans.back(), header);
    };

    auto emplace_remote_stream = [&]()
    {
        remote_shards.emplace_back(Shard{
            .query = modified_query_ast,
            .header = modified_header,
            .shard_num = shard_info.shard_num,
            .pool = shard_info.pool,
            .lazy = false
        });
    };

    const auto & settings = context->getSettingsRef();

    if (settings.prefer_localhost_replica && shard_info.isLocal())
    {
        StoragePtr main_table_storage;

        if (table_func_ptr)
        {
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_func_ptr, context);
            main_table_storage = table_function_ptr->execute(table_func_ptr, context, table_function_ptr->getName());
        }
        else
        {
            auto resolved_id = context->resolveStorageID(main_table);
            main_table_storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
        }


        if (!main_table_storage) /// Table is absent on a local server.
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            if (shard_info.hasRemoteConnections())
            {
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "There is no table {} on local replica of shard {}, will try remote replicas.",
                    main_table.getNameForLogs(), shard_info.shard_num);
                emplace_remote_stream();
            }
            else
                emplace_local_stream();  /// Let it fail the usual way.

            return;
        }

        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

        if (!replicated_storage)
        {
            /// Table is not replicated, use local server.
            emplace_local_stream();
            return;
        }

        UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

        if (!max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        UInt32 local_delay = replicated_storage->getAbsoluteDelay();

        if (local_delay < max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        /// If we reached this point, local replica is stale.
        ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"), "Local replica of shard {} is stale (delay: {}s.)", shard_info.shard_num, local_delay);

        if (!settings.fallback_to_stale_replicas_for_distributed_queries)
        {
            if (shard_info.hasRemoteConnections())
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            else
                throw Exception(
                    "Local replica of shard " + toString(shard_info.shard_num)
                    + " is stale (delay: " + toString(local_delay) + "s.), but no other replica configured",
                    ErrorCodes::ALL_REPLICAS_ARE_STALE);
        }

        if (!shard_info.hasRemoteConnections())
        {
            /// There are no remote replicas but we are allowed to fall back to stale local replica.
            emplace_local_stream();
            return;
        }

        /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
        /// Do it lazily to avoid connecting in the main thread.

        remote_shards.emplace_back(Shard{
            .query = modified_query_ast,
            .header = modified_header,
            .shard_num = shard_info.shard_num,
            .pool = shard_info.pool,
            .lazy = true,
            .local_delay = local_delay
        });
    }
    else
        emplace_remote_stream();
}

}
}
