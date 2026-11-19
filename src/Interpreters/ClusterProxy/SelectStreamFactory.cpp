#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/TableNode.h>
#include <Client/IConnections.h>
#include <Core/Settings.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageProxy.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/removeGroupingFunctionSpecializations.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
    extern const Event DistributedShardsSkipped;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
    extern const SettingsBool prefer_localhost_replica;
    extern const SettingsBool serialize_query_plan;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsUInt64 distributed_group_by_no_merge;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
}

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char use_delayed_remote_source[];
}

namespace ClusterProxy
{

namespace
{

/// Whether a condition on a table in `WHERE` keeps the same rows as a filter of the table before the join.
bool joinKeepsRowsOfTable(const ASTTableJoin & join, bool table_is_on_the_left)
{
    if (isCrossOrComma(join.kind))
        return true;

    if (table_is_on_the_left)
        return isLeft(join.kind) || (isInner(join.kind) && join.strictness != JoinStrictness::Any);

    return isInnerOrRight(join.kind) && (join.strictness == JoinStrictness::All || join.strictness == JoinStrictness::Unspecified);
}

bool hasPartitionIdColumn(const ASTPtr & ast)
{
    const auto * identifier = ast->as<ASTIdentifier>();
    if (identifier && identifier->shortName() == "_partition_id")
        return true;

    return std::ranges::any_of(ast->children, hasPartitionIdColumn);
}

/// The tables named as the remote table in the query tree for a shard, by the place of each of them.
struct TablesOfShardQuery
{
    /// Replaced a `Distributed` table in a `FROM` clause: they have no real storage.
    size_t replaced = 0;
    /// Replaced a `Distributed` table that is an argument of a function such as `IN`.
    size_t without_from = 0;
    /// Read directly by the query.
    size_t direct = 0;
};

void countTablesOfShardQuery(const QueryTreeNodePtr & node, const StorageID & table, bool is_function_argument, TablesOfShardQuery & tables)
{
    if (const auto * table_node = node->as<TableNode>())
    {
        const auto & storage_id = table_node->getStorageID();
        if (storage_id.table_name == table.table_name && storage_id.database_name == table.database_name)
        {
            if (!typeid_cast<const StorageDummy *>(table_node->getStorage().get()))
                ++tables.direct;
            else if (is_function_argument)
                ++tables.without_from;
            else
                ++tables.replaced;
        }
    }

    /// The arguments of a function are the elements of the lists under it.
    const bool children_are_function_arguments = node->as<FunctionNode>() || (is_function_argument && node->as<ListNode>());
    for (const auto & child : node->getChildren())
        if (child)
            countTablesOfShardQuery(child, table, children_are_function_arguments, tables);
}

/// Adds a condition on `_partition_id` to `WHERE` of every `SELECT` of the query for a shard that reads `table`, to skip `partitions`.
/// A setting of the query could be rejected or dropped by the constraints of the user. Returns the number of found tables.
size_t skipPartitionsInQueryForShard(const ASTPtr & ast, const StorageID & table, const Strings & partitions)
{
    size_t found = 0;

    auto * select_query = ast->as<ASTSelectQuery>();
    if (auto tables = select_query ? select_query->tables() : nullptr)
    {
        const Strings table_name = table.database_name.empty() ? Strings{table.table_name} : Strings{table.database_name, table.table_name};
        const auto & elements = tables->children;

        for (size_t i = 0; i < elements.size(); ++i)
        {
            const auto & table_expression = elements[i]->as<ASTTablesInSelectQueryElement &>().table_expression;
            if (!table_expression || !table_expression->as<ASTTableExpression &>().database_and_table_name)
                continue;

            const auto & identifier = table_expression->as<ASTTableExpression &>().database_and_table_name->as<ASTTableIdentifier &>();
            if (identifier.shortName() != table.table_name)
                continue;

            if (identifier.name_parts != table_name)
            {
                /// A name with and a name without a database are the same table or not, depending on the shard.
                if (identifier.name_parts.size() == 1 || table.database_name.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot tell if table {} in the query for a shard is table {} while its partitions are moved between shards",
                        identifier.name(), table.getNameForLogs());
                continue;
            }

            for (size_t j = std::max<size_t>(i, 1); j < elements.size(); ++j)
            {
                const auto & table_join = elements[j]->as<ASTTablesInSelectQueryElement &>().table_join;
                if (table_join && !joinKeepsRowsOfTable(table_join->as<ASTTableJoin &>(), j > i))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "This JOIN is not supported for table {} while its partitions are moved between shards", table.getNameForLogs());
            }

            Strings column = identifier.tryGetAlias().empty() ? identifier.name_parts : Strings{identifier.tryGetAlias()};
            column.push_back("_partition_id");

            ASTPtr filter = makeASTFunction("notIn",
                make_intrusive<ASTIdentifier>(std::move(column)), make_intrusive<ASTLiteral>(Array(partitions.begin(), partitions.end())));

            if (auto where = select_query->where())
                filter = makeASTFunction("and", where, filter);

            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(filter));
            ++found;
        }
    }

    for (const auto & child : ast->children)
        found += skipPartitionsInQueryForShard(child, table, partitions);

    return found;
}

}

SelectStreamFactory::SelectStreamFactory(
    SharedHeader header_,
    const StorageSnapshotPtr & storage_snapshot_,
    QueryProcessingStage::Enum processed_stage_)
    : header(header_),
    storage_snapshot(storage_snapshot_),
    processed_stage(processed_stage_)
{
}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const QueryTreeNodePtr & query_tree,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    QueryPlanPtrs & local_plans,
    Shards & remote_shards,
    UInt32 shard_count,
    bool parallel_replicas_enabled,
    AdditionalShardFilterGenerator shard_filter_generator,
    const UnavailableShardTrackerPtr & unavailable_shard_tracker,
    const Strings * partitions_to_skip)
{
    if (query.tree != query_tree)
    {
        /// Convert grouping function specializations (e.g. __groupingForGroupingSets -> grouping)
        /// so the AST contains the generic function name that the shard's analyzer can re-resolve.
        /// Use a clone to keep the original query_tree with specialized functions intact,
        /// since it is reused later for getSampleBlock / plan building.
        auto query_tree_for_ast = query_tree->clone();
        removeGroupingFunctionSpecializations(query_tree_for_ast);

        query = {.tree = query_tree, .ast = queryNodeToDistributedSelectQuery(query_tree_for_ast), .header = {}, .planner_context = {}};
    }

    /// The query of this shard: the shared one, unless some partitions are skipped on this shard.
    ASTPtr query_ast = query.ast;
    /// A query that reads `_partition_id` itself keeps the access check of the column.
    const bool check_partition_id = !partitions_to_skip || hasPartitionIdColumn(query_ast);
    if (partitions_to_skip)
    {
        query_ast = query_ast->clone();
        size_t found = skipPartitionsInQueryForShard(query_ast, main_table, *partitions_to_skip);
        if (!found)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot find table {} in the query for a shard while its partitions are moved between shards", main_table.getNameForLogs());

        /// Each table is judged by its own place: a direct read and a table without `FROM` together keep the totals equal.
        TablesOfShardQuery tables;
        countTablesOfShardQuery(query_tree, main_table, false, tables);

        /// A direct read of the table by the query is not told from a read through the `Distributed` table by its name.
        if (tables.direct || found > tables.replaced)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Table {} is read directly by a query that reads it through a `Distributed` table while its partitions are moved between shards",
                main_table.getNameForLogs());

        /// A table that is the right argument of `IN` has no `WHERE` to put the condition to.
        if (tables.without_from || found < tables.replaced)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Table {} is read by a query for a shard without a `FROM` clause while its partitions are moved between shards",
                main_table.getNameForLogs());
    }
    const StorageID * table_filtered_by_partition_id = check_partition_id ? nullptr : &main_table;

    ContextPtr context_without_parallel_replicas;

    /// `local_storage` is the table the local plan will read, null when it is not resolved.
    auto emplace_local_stream = [&](const StoragePtr & local_storage)
    {
        /// A local plan does not go through `ReadFromRemote`, so nothing sets `cluster_for_parallel_replicas`
        /// to this hop's cluster and the read would be scoped by another one. Keep parallel replicas only
        /// for a nested `Distributed`, which sets up its own cluster. A `View` over a `Distributed` is left
        /// out: it reads without parallel replicas, as it did before.
        auto local_context = context;
        if (context->canUseTaskBasedParallelReplicas()
            && !castStorage<StorageDistributed>(local_storage, DeferredTable::Load))
        {
            if (!context_without_parallel_replicas)
            {
                auto mutable_context = Context::createCopy(context);
                Settings settings_without_parallel_replicas = mutable_context->getSettingsCopy();
                settings_without_parallel_replicas[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
                mutable_context->setSettings(settings_without_parallel_replicas);
                context_without_parallel_replicas = std::move(mutable_context);
            }
            local_context = context_without_parallel_replicas;
        }

        local_plans.emplace_back(createLocalPlan(
            query_ast->clone(), *header, local_context, processed_stage, shard_info.shard_num, shard_count, false, {}, table_filtered_by_partition_id));
    };

    // If lazy is true, a lazy pipe will be created. It will try to use the local replica and, if not possible, will use DelayedSource for reading from remote replica.
    auto emplace_remote_stream = [&](bool lazy = false)
    {
        SharedHeader shard_header;
        PlannerContextPtr planner_context;
        std::unique_ptr<QueryPlan> query_plan;

        const auto & settings = context->getSettingsRef();

        /// Disable for distributed_group_by_no_merge now, because distributed-over-distributed only works up to FetchColumns,
        /// But distributed_group_by_no_merge requires Complete.
        if (settings[Setting::serialize_query_plan] && !settings[Setting::distributed_group_by_no_merge])
        {
            query_plan = createLocalPlan(
                query_ast->clone(), *header, context, processed_stage, shard_info.shard_num, shard_count, true, shard_info.default_database,
                table_filtered_by_partition_id);

            shard_header = query_plan->getCurrentHeader();
        }
        else
        {
            /// The same for all shards with this query tree, so do not plan the query again for each of them.
            if (!query.header)
                std::tie(query.header, query.planner_context) = InterpreterSelectQueryAnalyzer::getSampleBlockAndPlannerContext(query_tree, context, SelectQueryOptions(processed_stage).analyze());

            shard_header = query.header;
            planner_context = query.planner_context;
        }

        /// Strip initiator-only settings from the query text forwarded to the shard. The AST carries them
        /// from a nested `SETTINGS` clause, and on the analyzer path from `QueryNode::settings_changes`,
        /// which `queryNodeToDistributedSelectQuery` (`QueryNode::toAST`) materializes into the SELECT's
        /// `SETTINGS`. They are irrelevant to the remote query and can trip `UNKNOWN_SETTING` on an older
        /// shard during a rolling upgrade; the inter-server settings packet is stripped separately in
        /// `updateSettings`. The local plan (`emplace_local_stream`) keeps the unstripped query.
        auto forwarded_query = query_ast->clone();
        stripInitiatorOnlySettingsFromQuery(forwarded_query);

        remote_shards.emplace_back(Shard{
            .query = forwarded_query,
            .query_tree = query_tree,
            .planner_context = planner_context,
            .query_plan = std::move(query_plan),
            .main_table = main_table,
            .header = shard_header,
            .shard_info = shard_info,
            .lazy = lazy,
            .main_table_filtered_by_partition_id = table_filtered_by_partition_id != nullptr,
            .shard_filter_generator = std::move(shard_filter_generator),
        });
    };

    const auto & settings = context->getSettingsRef();

    fiu_do_on(FailPoints::use_delayed_remote_source,
    {
        emplace_remote_stream(/*lazy=*/true);
        return;
    });

    // prefer_localhost_replica is not effective in case of parallel replicas
    // (1) prefer_localhost_replica is about choosing one replica on a shard
    // (2) parallel replica coordinator has own logic to choose replicas to read from
    if (settings[Setting::prefer_localhost_replica] && shard_info.isLocal() && !parallel_replicas_enabled)
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
                LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"),
                    "There is no table {} on local replica of shard {}, will try remote replicas.",
                    main_table.getNameForLogs(), shard_info.shard_num);
                emplace_remote_stream();
            }
            else if (settings[Setting::skip_unavailable_shards])
            {
                LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"),
                    "There is no table {} on local replica of shard {}, and no remote replicas configured. Skipping.",
                    main_table.getNameForLogs(), shard_info.shard_num);
                ProfileEvents::increment(ProfileEvents::DistributedShardsSkipped);
                if (unavailable_shard_tracker)
                    unavailable_shard_tracker->onShardSkipped();
            }
            else
                emplace_local_stream(main_table_storage);  /// Let it fail the usual way.

            return;
        }

        const auto * replicated_storage = castStorage<StorageReplicatedMergeTree>(main_table_storage, DeferredTable::Load).get();

        if (!replicated_storage)
        {
            /// Table is not replicated, use local server.
            emplace_local_stream(main_table_storage);
            return;
        }

        const UInt64 max_allowed_delay = settings[Setting::max_replica_delay_for_distributed_queries];

        if (!max_allowed_delay)
        {
            emplace_local_stream(main_table_storage);
            return;
        }

        UInt64 local_delay = replicated_storage->getAbsoluteDelay();

        if (local_delay < max_allowed_delay)
        {
            emplace_local_stream(main_table_storage);
            return;
        }

        /// If we reached this point, local replica is stale.
        ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"), "Local replica of shard {} is stale (delay: {}s.)", shard_info.shard_num, local_delay);

        if (!settings[Setting::fallback_to_stale_replicas_for_distributed_queries])
        {
            if (shard_info.hasRemoteConnections())
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            throw Exception(
                ErrorCodes::ALL_REPLICAS_ARE_STALE,
                "Local replica of shard {} is stale (delay: "
                "{}s.), but no other replica configured",
                shard_info.shard_num,
                toString(local_delay));
        }

        if (!shard_info.hasRemoteConnections())
        {
            /// There are no remote replicas but we are allowed to fall back to stale local replica.
            emplace_local_stream(main_table_storage);
            return;
        }

        /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
        /// Do it lazily to avoid connecting in the main thread.
        emplace_remote_stream(true /* lazy */);
    }
    else
        emplace_remote_stream();
}


}
}
