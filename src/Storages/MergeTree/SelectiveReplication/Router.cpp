#include <Storages/MergeTree/SelectiveReplication/Router.h>

#include <Storages/StorageReplicatedMergeTree.h>
#include <Core/Settings.h>
#include <Interpreters/InterserverCredentials.h>
#include <Storages/RemoteQueryCommon.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageDummy.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/buildQueryTreeForShard.h>

#include <Common/isLocalAddress.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Poco/Net/SocketAddress.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/Cluster.h>

#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ListNode.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <DataTypes/DataTypeString.h>

#include <Planner/Utils.h>
#include <Client/ConnectionPool.h>

#include <Storages/Distributed/DistributedSettings.h>

#include <string_view>
#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event SelectiveReplicationSelectForwardedQueries;
    extern const Event SelectiveReplicationSelectForwardedPartitions;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 select_sequential_consistency;
    extern const SettingsUInt64 max_distributed_depth;
}

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int ALL_REPLICAS_LOST;
    extern const int NO_ZOOKEEPER;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
}

namespace SelectiveReplication
{

namespace
{
constexpr std::string_view SELECTIVE_REPLICATION_ADDITIONAL_FILTER_MARKER = "__clickhouse_selective_replication_local_filter";

bool isSelectiveReplicationAdditionalFilterMarker(const Field & additional_filter, const StorageID & storage_id)
{
    const auto & tuple = additional_filter.safeGet<Tuple>();
    return tuple.size() == 2
        && tuple.at(0).safeGet<String>() == SELECTIVE_REPLICATION_ADDITIONAL_FILTER_MARKER
        && tuple.at(1).safeGet<String>() == storage_id.getFullNameNotQuoted();
}
}

Router::Router(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(storage_.log.load())
{
}

std::unordered_map<String, ReplicatedMergeTreeAddress> Router::getReplicaAddressesCached(
    const Strings & replicas) const
{
    /// Step 1: check cache under lock, return only the requested replica subset.
    {
        std::lock_guard lock(address_cache_mutex);

        auto now = std::chrono::steady_clock::now();
        if (now - cached_addresses.last_refresh < ADDRESS_CACHE_TTL && !cached_addresses.addresses.empty())
        {
            bool all_cached = true;
            for (const auto & r : replicas)
            {
                if (!cached_addresses.addresses.contains(r))
                {
                    all_cached = false;
                    break;
                }
            }
            if (all_cached)
            {
                /// Return only requested replicas, not the full cache.
                std::unordered_map<String, ReplicatedMergeTreeAddress> result;
                result.reserve(replicas.size());
                for (const auto & r : replicas)
                    result[r] = cached_addresses.addresses[r];
                return result;
            }
        }
    }

    /// Step 2: ZK I/O outside the lock to avoid blocking concurrent queries.
    auto zk = storage.getZooKeeper();
    auto fresh = getReplicaAddresses(replicas, zk);

    /// Step 3: merge into cache (not replace) to preserve entries added by concurrent calls.
    {
        std::lock_guard lock(address_cache_mutex);
        for (auto & [name, addr] : fresh)
            cached_addresses.addresses[name] = addr;
        cached_addresses.last_refresh = std::chrono::steady_clock::now();
    }

    return fresh;
}

std::optional<QueryProcessingStage::Enum> Router::resolveQueryProcessingStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    /// Guard: selective routing is incompatible with parallel replicas.
    if (to_stage < QueryProcessingStage::WithMergeableState
        || query_context->canUseParallelReplicasCustomKey()
        || query_context->canUseTaskBasedParallelReplicas())
    {
        return std::nullopt;
    }

    auto depth = query_context->getClientInfo().distributed_depth;

    if (depth == 0)
    {
        /// depth==0 path: full assignment-based stage resolution.
        return resolveSelectiveReplicationStage(query_context, to_stage, storage_snapshot, query_info);
    }

    /// depth > 0: mirror `reEnterSelectiveRoutingAtDepth`'s branching.
    /// If any received partition is misplaced (cache says it belongs elsewhere),
    /// the re-route path will go through `readWithSelectiveRouting` →
    /// `ReadFromRemote`, whose header is shaped for `WithMergeableState`. The
    /// outer Planner must agree on stage, otherwise its FetchColumns post-read
    /// rename step (PlannerJoinTree.cpp:1427) will drop every identifier column.
    /// If all owned locally, fall through to the default stage — the local read
    /// returns raw columns, which matches FetchColumns.
    if (depth < SelectiveReplication::MAX_FORWARDING_DEPTH
        && hasMisplacedReceivedPartitions(query_info, query_context))
    {
        return QueryProcessingStage::WithMergeableState;
    }

    /// All received partitions are owned locally — fall through to default stage.
    return std::nullopt;
}


std::optional<QueryProcessingStage::Enum> Router::resolveSelectiveReplicationStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    /// When selective replication routes to multiple replicas via ClusterProxy, each replica
    /// executes up to `WithMergeableState`. If all assigned partitions are local (`all_local`),
    /// fall through to the default stage — `readLocalImpl` returns raw columns, not intermediate
    /// aggregate state, so `WithMergeableState` must not be claimed in that case.

    /// If `additional_table_filters` carries the selective replication local marker for this table,
    /// the query has already been routed by a parent selective replication router. The local read path
    /// (via createLocalPlan) should not claim WithMergeableState, otherwise it would
    /// re-enter readWithSelectiveRouting and recurse indefinitely.
    const auto & additional_filters = query_context->getSettingsRef()[Setting::additional_table_filters].value;
    if (!additional_filters.empty())
    {
        auto storage_id = storage.getStorageID();
        for (const auto & additional_filter : additional_filters)
        {
            if (isSelectiveReplicationAdditionalFilterMarker(additional_filter, storage_id))
            {
                LOG_DEBUG(log, "resolveSelectiveReplicationStage: selective replication local filter marker found in "
                    "additional_table_filters, falling through to local read (selective replication routing already applied)");
                return std::nullopt;
            }
        }
    }

    if (query_context->getSettingsRef()[Setting::select_sequential_consistency])
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "select_sequential_consistency is not supported with selective replication "
            "(replication_factor > 0). Either disable select_sequential_consistency or "
            "set replication_factor to 0.");
    }

    /// Read assignment data from the snapshot — populated during createStorageSnapshot.
    const auto * snapshot_data = assert_cast<const StorageReplicatedMergeTree::SnapshotData *>(storage_snapshot->data.get());
    const auto & assignment_map = snapshot_data->selective_assignment_map;

    if (assignment_map.empty())
    {
        /// Snapshot has no assignment data — ZK was likely unreachable during snapshot creation.
        /// Respect skip_unavailable_shards.
        if (query_context->getSettingsRef()[Setting::skip_unavailable_shards])
        {
            LOG_WARNING(log, "resolveSelectiveReplicationStage: no assignment data in snapshot, "
                "falling back to local read");
            return storage.MergeTreeData::getQueryProcessingStage(query_context, to_stage, storage_snapshot, query_info);
        }
        /// Without skip_unavailable_shards, fall through to default stage.
        /// The table may not have any partitions yet, so empty is not necessarily an error.
        return std::nullopt;
    }

    bool has_remote = false;
    for (const auto & [partition_id, assigned_replicas] : assignment_map)
    {
        if (assigned_replicas.empty())
            continue; /// unassigned — falls back to local
        if (!KeeperReplicaAssignment::isReplicaAssigned(assigned_replicas, storage.replica_name))
        {
            has_remote = true;
            break;
        }
    }

    if (has_remote)
        return QueryProcessingStage::WithMergeableState;
    /// else: all_local — fall through to MergeTreeData default
    return std::nullopt;
}


void Router::readWithSelectiveRouting(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const std::unordered_map<String, Strings> * override_assignment_map)
{
    const auto & settings = local_context->getSettingsRef();

    /// Assignment map selection:
    ///   - depth=0 entry: use the snapshot's map (computed at createStorageSnapshot).
    ///   - depth>0 entry (reEnterSelectiveRoutingAtDepth): use the override, which is
    ///     restricted to the partition subset received from the upstream replica plus
    ///     the owned subset pointed back at self.
    const auto * snapshot_data = assert_cast<const StorageReplicatedMergeTree::SnapshotData *>(storage_snapshot->data.get());
    const auto & assignment_map = override_assignment_map
        ? *override_assignment_map
        : snapshot_data->selective_assignment_map;

    if (assignment_map.empty())
    {
        /// Snapshot has no assignment data — fall through to local read.
        storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info, local_context, max_block_size, num_streams);
        return;
    }

    /// Step 1: Build replica -> partition_ids routing map.
    /// When running at depth>0 with an override map, restrict local-fallback detection
    /// to the same subset so we don't accidentally pull unrelated local partitions in.
    std::unordered_set<String> local_partition_ids_set;
    std::optional<std::unordered_set<String>> pid_restriction;
    if (override_assignment_map)
    {
        pid_restriction.emplace();
        pid_restriction->reserve(override_assignment_map->size());
        for (const auto & [pid, _] : *override_assignment_map)
            pid_restriction->insert(pid);
    }
    auto partitions_by_replica = buildPartitionRoutingMap(
        assignment_map, local_partition_ids_set,
        pid_restriction.has_value() ? &*pid_restriction : nullptr);

    bool all_local = partitions_by_replica.empty()
        || (partitions_by_replica.size() == 1 && partitions_by_replica.contains(storage.replica_name));

    if (all_local)
    {
        LOG_DEBUG(log, "Selective replication: all assigned partitions route to local replica, reading locally");
        storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info, local_context, max_block_size, num_streams);
        return;
    }

    /// Step 2: Resolve replica addresses and handle failures.
    auto zk = storage.getZooKeeper();
    Strings target_replicas;
    target_replicas.reserve(partitions_by_replica.size());
    for (const auto & [target_replica, _] : partitions_by_replica)
        target_replicas.push_back(target_replica);
    auto address_map = getReplicaAddressesCached(target_replicas);

    redistributeFailedReplicas(partitions_by_replica, address_map, assignment_map, local_partition_ids_set, settings);

    /// Step 3: Prepare remote execution context.
    /// Do not short-circuit to `readLocalImpl` even for single-server setups: when
    /// `getQueryProcessingStage` returns `WithMergeableState`, the Planner expects
    /// intermediate aggregate state. `readLocalImpl` returns raw columns, causing
    /// `CANNOT_CONVERT_TYPE`. Use ClusterProxy with `treat_local_as_remote=false`
    /// so local replicas go through `createLocalPlan` with `additional_table_filters`.

    auto credentials = storage.getContext()->getInterserverCredentials();
    String cluster_user = credentials->getUser();
    String cluster_password = credentials->getPassword();

    UInt16 default_tcp_port = storage.getContext()->getTCPPort();
    String bind_host;

    bool secure = (storage.getContext()->getInterserverScheme() == "https");
    ClusterConnectionParameters cluster_params{
        cluster_user,
        cluster_password,
        default_tcp_port,
        false /* treat_local_as_remote: local path uses additional_table_filters
                for partition filtering, remote path uses shard_filter_generator */,
        true /* treat_local_port_as_remote */,
        secure,
        bind_host,
        Priority{1},
        {} /* cluster_name */,
        {} /* cluster_secret */};

    /// Context for remote shards: depth is incremented so downstream replicas
    /// know the query was forwarded. The _partition_id filter is injected via
    /// shard_filter (SQL WHERE clause), so it appears in filter_actions_dag on
    /// the receiving end.
    auto remote_context = Context::createCopy(local_context);
    remote_context->increaseDistributedDepth();

    /// Context for local shard: depth is NOT incremented because this replica
    /// already confirmed these partitions are local (no re-routing needed).
    /// The _partition_id filter is injected via additional_table_filters,
    /// which does NOT populate filter_actions_dag — but that's fine because
    /// depth==0 prevents reEnterSelectiveRoutingAtDepth from being called.
    auto local_read_context = Context::createCopy(local_context);
    auto storage_id = storage.getStorageID();
    const auto & local_pids = partitions_by_replica[storage.replica_name];
    if (!local_pids.empty())
    {
        Array partition_array(local_pids.begin(), local_pids.end());
        auto filter = makeASTFunction("in",
            make_intrusive<ASTIdentifier>("_partition_id"),
            make_intrusive<ASTLiteral>(Field(partition_array)));
        const String selective_replication_filter = filter->formatWithSecretsOneLine();
        const String table_alias = query_info.table_expression ? query_info.table_expression->getAlias() : String{};

        Map filter_map = local_context->getSettingsRef()[Setting::additional_table_filters].value;
        bool merged_with_existing_filter = false;
        for (auto & additional_filter : filter_map)
        {
            auto & tuple = additional_filter.safeGet<Tuple>();
            const auto & table = tuple.at(0).safeGet<String>();
            if (additionalTableFilterMatches(table, storage_id, table_alias, local_context))
            {
                const auto & existing_filter = tuple.at(1).safeGet<String>();
                tuple[1] = String("(") + existing_filter + ") AND (" + selective_replication_filter + ")";
                merged_with_existing_filter = true;
                break;
            }
        }

        if (!merged_with_existing_filter)
            filter_map.push_back(Tuple{storage_id.getShortName(), selective_replication_filter});

        filter_map.push_back(Tuple{String(SELECTIVE_REPLICATION_ADDITIONAL_FILTER_MARKER), storage_id.getFullNameNotQuoted()});
        local_read_context->setSetting("additional_table_filters", Field(filter_map));
    }

    /// Step 4: Prepare query tree for shard distribution.
    /// Replace the table expression with `StorageDummy` so the Analyzer produces correct
    /// column aliases for remote nodes (same pattern as `StorageDistributed::buildQueryTreeDistributed`).
    if (!query_info.query_tree || !query_info.table_expression)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Selective replication read routing requires the query analyzer. "
            "SET enable_analyzer = 1");
    }

    auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);
    auto column_names_and_types = storage_snapshot->getColumns(get_column_options);
    auto dummy_storage = std::make_shared<StorageDummy>(storage_id, ColumnsDescription{column_names_and_types});
    auto replacement_table = std::make_shared<TableNode>(std::move(dummy_storage), local_context);

    if (auto * query_table_node = query_info.table_expression->as<TableNode>())
    {
        auto table_expression_modifiers = query_table_node->getTableExpressionModifiers();
        if (table_expression_modifiers)
            replacement_table->setTableExpressionModifiers(*table_expression_modifiers);
    }
    replacement_table->setAlias(query_info.table_expression->getAlias());

    auto query_tree_for_shard = query_info.query_tree->cloneAndReplace(
        query_info.table_expression, std::move(replacement_table));

    ReplaceAliasColumnsVisitor replace_alias_columns_visitor;
    replace_alias_columns_visitor.visit(query_tree_for_shard);

    query_tree_for_shard = buildQueryTreeForShard(
        query_info.planner_context, query_tree_for_shard,
        /*allow_global_join_for_right_target*/ false);

    /// Compute the shared header from the query tree (without partition filter).
    Block header_block = *InterpreterSelectQueryAnalyzer::getSampleBlock(
        query_tree_for_shard, local_context, SelectQueryOptions(processed_stage).analyze());
    for (auto & column : header_block)
        column.column = column.column->convertToFullColumnIfConst();
    SharedHeader header = std::make_shared<const Block>(std::move(header_block));

    /// Step 5: Build per-shard plans with partition filters.
    size_t shard_count = partitions_by_replica.size();

    std::vector<QueryPlanPtr> plans;
    ClusterProxy::SelectStreamFactory::Shards remote_shards;

    for (const auto & [target_replica, partition_ids] : partitions_by_replica)
    {
        if (partition_ids.empty())
            continue;
        auto it = address_map.find(target_replica);
        /// After redistribution every remaining entry must have a valid address
        /// (the local replica needs no address lookup).
        chassert(it != address_map.end() || target_replica == storage.replica_name);
        if (it == address_map.end())
        {
            LOG_WARNING(log, "Selective replication: SKIP replica {} (not in address_map), partitions=[{}]",
                target_replica, fmt::join(partition_ids, ","));
            continue;
        }
        const auto & address = it->second;

        LOG_DEBUG(log, "Selective routing: CREATE shard for replica {} partitions=[{}] address={}:{}",
            target_replica, fmt::join(partition_ids, ","), address.host, address.queries_port);

        DatabaseReplicaInfo info;
        info.hostname = address.host + ":" + toString(address.queries_port);
        info.shard_name = target_replica;
        info.replica_name = target_replica;

        auto per_shard_cluster = std::make_shared<Cluster>(
            local_context->getSettingsRef(),
            std::vector<std::vector<DatabaseReplicaInfo>>{{info}},
            cluster_params);

        ClusterProxy::SelectStreamFactory select_stream_factory(
            header, storage_snapshot, processed_stage);

        auto filter_pids = partition_ids;
        auto shard_filter = [filter_pids](UInt64) -> ASTPtr
        {
            return DB::buildPartitionFilterAST(filter_pids);
        };

        select_stream_factory.createForShard(
            per_shard_cluster->getShardsInfo()[0],
            query_tree_for_shard,
            storage_id,
            nullptr /* table_func_ptr */,
            target_replica == storage.replica_name ? local_read_context : remote_context,
            plans,
            remote_shards,
            static_cast<UInt32>(shard_count),
            false /* parallel_replicas_enabled */,
            false /* use_parallel_replicas_custom_key */,
            shard_filter);
    }

    /// Step 6: Combine remote shards into a `ReadFromRemote` plan step.
    if (!remote_shards.empty())
    {
        plans.emplace_back(createReadFromRemotePlan(
            std::move(remote_shards),
            header,
            processed_stage,
            storage_id,
            nullptr /* table_func_ptr */,
            remote_context,
            local_context,
            log,
            static_cast<UInt32>(shard_count),
            query_info.storage_limits,
            "" /* cluster_name */,
            "Read from selective replication remote replica",
            nullptr /* throttler */,
            nullptr /* unavailable_shard_tracker */));
    }

    /// Step 7: Unite plans.
    if (plans.empty())
    {
        storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info, local_context, max_block_size, num_streams);
        return;
    }

    const auto plan_count = plans.size();
    unitePlanList(query_plan, std::move(plans));

    if (plan_count == 1)
    {
        LOG_DEBUG(log, "Selective replication: routed query to 1 shard");
        return;
    }

    LOG_DEBUG(log, "Selective replication: routed query to {} shards via PREWHERE partition filters", shard_count);
}


void Router::reEnterSelectiveRoutingAtDepth(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    const int depth = static_cast<int>(local_context->getClientInfo().distributed_depth);

    /// Step 1: Extract the _partition_id filter set from the query tree.
    /// At depth>0 the selective replication upstream router normally injects a _partition_id filter
    /// (via shard_filter or additional_table_filters). Its absence means the query
    /// arrived through a non-selective-replication path, e.g. a Distributed engine query that hashed
    /// this replica and forwarded the sub-query unchanged. In that case, re-running
    /// the full selective routing path here would rebuild the query tree from
    /// `query_info.query_tree`, which already has Distributed-engine-specific
    /// transformations applied; the produced remote sub-queries would carry a
    /// different header (e.g. `_partition_id` virtual column) than the one the
    /// upstream Distributed executor is expecting, yielding NOT_FOUND_COLUMN_IN_BLOCK.
    ///
    /// The conservative choice is to fall back to a plain local read on this
    /// replica. This preserves correctness for all selective-replication-originated routing paths
    /// (which always carry a pid filter at depth>0) and matches the behavior
    /// of a non-selective-replication Replicated table when accessed through Distributed: each
    /// shard replica reads whatever parts it holds locally. Callers that rely
    /// on full data visibility through Distributed should either disable selective replication
    /// or use the selective-replication-aware SELECT path (direct table access with
    /// `allow_experimental_analyzer=1`).
    auto received_pids = extractPartitionIdsFromQueryInfo(query_info, local_context);
    if (received_pids.empty())
    {
        LOG_DEBUG(log, "Selective replication (depth={}): no _partition_id filter from "
                       "upstream (likely Distributed engine or non-selective-replication forward path); "
                       "falling back to local read.", depth);
        storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info,
                              local_context, max_block_size, num_streams);
        return;
    }

    LOG_DEBUG(log, "Selective replication (depth={}): received {} partition(s) for re-routing check",
        depth, received_pids.size());

    /// Step 2: Pure in-memory cache lookup. zk=nullptr + force_refresh=false = no ZK IO.
    std::vector<String> received_pids_vec(received_pids.begin(), received_pids.end());
    auto sub_assignments = storage.replica_assignment->getAssignments(
        /*zk=*/ nullptr, received_pids_vec, /*force_refresh=*/ false);

    /// Step 3: Split owned vs misplaced.
    /// - Cache hit with storage.replica_name in assigned list → owned.
    /// - Cache hit with storage.replica_name NOT in assigned list → misplaced (re-route to current owner).
    /// - Cache miss (no entry or empty replicas) → treat as owned, fall back to local read.
    ///   Covers cold cache / pre-selective-replication data / migration edge cases. Correctness: if the local
    ///   replica has no parts for the pid, the local read returns empty — identical to
    ///   what we'd get by forwarding.
    std::unordered_map<String, Strings> misplaced_assignment_map;
    Strings owned_pids;
    owned_pids.reserve(received_pids.size());
    for (const auto & pid : received_pids)
    {
        auto it = sub_assignments.find(pid);
        if (it == sub_assignments.end() || it->second.replicas.empty())
        {
            owned_pids.push_back(pid);
            continue;
        }
        if (KeeperReplicaAssignment::isReplicaAssigned(it->second.replicas, storage.replica_name))
            owned_pids.push_back(pid);
        else
            misplaced_assignment_map[pid] = it->second.replicas;
    }

    /// Step 4: Fast path — every received pid is owned or cache-miss falls local.
    if (misplaced_assignment_map.empty())
    {
        LOG_DEBUG(log, "Selective replication (depth={}): all {} received partition(s) owned "
            "locally or fall through to local read", depth, received_pids.size());
        storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info,
                      local_context, max_block_size, num_streams);
        return;
    }

    /// Step 4b: Self-loop detection.
    /// Resolve target replica addresses and detect pids whose candidate owners all
    /// point back to our own host:queries_port. This happens in single-server
    /// deployments (multiple replicas registered in ZK but hosted by the same
    /// process) where re-forwarding would bounce the query back to ourselves and
    /// trigger an infinite depth-forwarding loop. Treat such pids as owned and let
    /// the local read path apply the partition filter — parts we don't hold simply
    /// return empty rows, which matches the behavior the upstream router would see
    /// from a real remote replica.
    {
        Strings candidate_replicas;
        candidate_replicas.reserve(misplaced_assignment_map.size() * 2);
        std::unordered_set<String> seen;
        for (const auto & [pid, replicas] : misplaced_assignment_map)
        {
            for (const auto & r : replicas)
            {
                auto clean = KeeperReplicaAssignment::stripCloningSuffix(r);
                if (clean != storage.replica_name && seen.insert(clean).second)
                    candidate_replicas.push_back(clean);
            }
        }

        if (!candidate_replicas.empty())
        {
            auto zk_for_lookup = storage.tryGetZooKeeper();
            if (zk_for_lookup)
            {
                Strings candidate_replicas_vec(candidate_replicas.begin(), candidate_replicas.end());
                /// Guard the ZK multiRead inside `getReplicaAddresses` against a
                /// missing Coordination component. Depending on call site, the
                /// current thread may not have one set (see also the twin guard in
                /// `hasMisplacedReceivedPartitions`). Cheap no-op if already set.
                auto component_guard = Coordination::setCurrentComponent(
                    "SelectiveReplication::Router::reEnterSelectiveRoutingAtDepth");
                try
                {
                    auto addr_map = getReplicaAddressesCached(candidate_replicas_vec);
                    const UInt16 my_tcp_port = storage.getContext()->getTCPPort();
                    std::unordered_set<String> self_loop_replicas;
                    for (const auto & [rep, addr] : addr_map)
                    {
                        try
                        {
                            if (isLocalAddress(Poco::Net::SocketAddress(addr.host, addr.queries_port), my_tcp_port))
                                self_loop_replicas.insert(rep);
                        }
                        catch (const Poco::Exception &)
                        {
                            /// DNS or address resolution failure — skip this candidate.
                            tryLogCurrentException(log, fmt::format("Failed to resolve address for replica '{}' during self-loop check", rep), LogsLevel::warning);
                        }
                    }

                    if (!self_loop_replicas.empty())
                    {
                        Strings reclassified;
                        for (auto it = misplaced_assignment_map.begin(); it != misplaced_assignment_map.end();)
                        {
                            bool all_self_loop = true;
                            for (const auto & r : it->second)
                            {
                                auto clean = KeeperReplicaAssignment::stripCloningSuffix(r);
                                if (!self_loop_replicas.contains(clean))
                                {
                                    all_self_loop = false;
                                    break;
                                }
                            }
                            if (all_self_loop)
                            {
                                reclassified.push_back(it->first);
                                owned_pids.push_back(it->first);
                                it = misplaced_assignment_map.erase(it);
                            }
                            else
                            {
                                ++it;
                            }
                        }

                        if (!reclassified.empty())
                        {
                            LOG_DEBUG(log, "Selective replication (depth={}): self-loop detected — "
                                "{} partition(s) would forward back to the same host:port; "
                                "falling through to local read for [{}]",
                                depth, reclassified.size(), fmt::join(reclassified, ","));
                        }

                        if (misplaced_assignment_map.empty())
                        {
                            /// All misplaced partitions resolved to self-loop replicas.
                            /// Try to find the sibling storage that actually owns the data.
                            if (self_loop_replicas.size() == 1)
                            {
                                if (auto sibling = findSiblingStorage(*self_loop_replicas.begin()))
                                {
                                    LOG_DEBUG(log, "Selective replication (depth={}): self-loop fallback "
                                        "delegating to sibling storage (replica '{}')",
                                        depth, sibling->getReplicaName());
                                    auto sibling_snapshot = sibling->getStorageSnapshot(
                                        sibling->getInMemoryMetadataPtr(local_context, false), local_context);
                                    sibling->readLocalImpl(query_plan, column_names, sibling_snapshot,
                                                           query_info, local_context, max_block_size, num_streams);
                                    return;
                                }
                            }
                            storage.readLocalImpl(query_plan, column_names, storage_snapshot, query_info,
                                          local_context, max_block_size, num_streams);
                            return;
                        }
                    }
                }
                catch (const DB::Exception &)
                {
                    /// ZK error during address lookup — respect skip_unavailable_shards.
                    /// If enabled, treat misplaced partitions as local (conservative: partial
                    /// results are better than query failure). Otherwise propagate the error.
                    if (local_context->getSettingsRef()[Setting::skip_unavailable_shards])
                    {
                        LOG_WARNING(log, "ZK error during self-loop check at depth={}, "
                            "skip_unavailable_shards=true: treating {} misplaced partition(s) as local",
                            depth, misplaced_assignment_map.size());
                        for (auto & [pid, _] : misplaced_assignment_map)
                            owned_pids.push_back(pid);
                        misplaced_assignment_map.clear();
                    }
                    else
                    {
                        tryLogCurrentException(log, "ZK error during self-loop check in reEnterSelectiveRoutingAtDepth", LogsLevel::warning);
                        throw;
                    }
                }
            }
            else
            {
                /// ZK unavailable — cannot resolve addresses to detect self-loops.
                /// Respect skip_unavailable_shards: if enabled, treat all misplaced
                /// partitions as local (conservative fallback). Otherwise throw.
                if (local_context->getSettingsRef()[Setting::skip_unavailable_shards])
                {
                    LOG_WARNING(log, "ZK unavailable during self-loop detection at depth={}, "
                        "skip_unavailable_shards=true: treating {} misplaced partition(s) as local",
                        depth, misplaced_assignment_map.size());
                    for (auto & [pid, _] : misplaced_assignment_map)
                        owned_pids.push_back(pid);
                    misplaced_assignment_map.clear();
                }
                else
                {
                    throw Exception(ErrorCodes::NO_ZOOKEEPER,
                        "Cannot resolve replica addresses for selective replication routing: ZooKeeper unavailable");
                }
            }
        }
    }

    /// Step 5: Depth gate — symmetric with INSERT Phase 2 (ReplicatedMergeTreeSink.cpp:680)
    /// and capped by `max_distributed_depth` when the setting is enabled.
    const auto max_distributed_depth = local_context->getSettingsRef()[Setting::max_distributed_depth].value;
    const auto max_forwarding_depth = max_distributed_depth
        ? static_cast<int>(std::min<UInt64>(SelectiveReplication::MAX_FORWARDING_DEPTH, max_distributed_depth))
        : SelectiveReplication::MAX_FORWARDING_DEPTH;
    if (depth >= max_forwarding_depth)
    {
        Strings misplaced_pids;
        misplaced_pids.reserve(misplaced_assignment_map.size());
        for (const auto & [pid, _] : misplaced_assignment_map)
            misplaced_pids.push_back(pid);
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH,
            "Selective replication: SELECT cannot re-route {} misplaced partition(s) — "
            "exceeded max forwarding depth (effective={}, internal={}, max_distributed_depth={}). distributed_depth={}. "
            "Misplaced partitions: [{}]",
            misplaced_assignment_map.size(),
            max_forwarding_depth, SelectiveReplication::MAX_FORWARDING_DEPTH, max_distributed_depth, depth,
            fmt::join(misplaced_pids, ","));
    }

    /// Step 6: Build the override map: misplaced pids point at their current owner,
    /// owned pids point at self. buildPartitionRoutingMap's existing logic routes
    /// each entry correctly (self-pointing entries go to local; others go to the
    /// hash-picked replica).
    std::unordered_map<String, Strings> override_map = misplaced_assignment_map;
    for (const auto & pid : owned_pids)
        override_map[pid] = {storage.replica_name};

    LOG_DEBUG(log, "Selective replication (depth={}): re-routing {} misplaced partition(s), "
        "reading {} owned partition(s) locally",
        depth, misplaced_assignment_map.size(), owned_pids.size());

    ProfileEvents::increment(ProfileEvents::SelectiveReplicationSelectForwardedQueries);
    ProfileEvents::increment(ProfileEvents::SelectiveReplicationSelectForwardedPartitions,
        misplaced_assignment_map.size());

    /// Step 7: Delegate to the parameterized readWithSelectiveRouting. It reuses all
    /// of Steps 2-7 from the existing routing implementation (address resolution,
    /// redistribution, shard plans, ReadFromRemote, UnionStep).
    readWithSelectiveRouting(query_plan, column_names, storage_snapshot, query_info,
                             local_context, processed_stage, max_block_size, num_streams,
                             /*override_assignment_map=*/ &override_map);
}


std::unordered_map<String, Strings> Router::buildPartitionRoutingMap(
    const std::unordered_map<String, Strings> & assignment_map,
    std::unordered_set<String> & local_partition_ids_set,
    const std::unordered_set<String> * pid_restriction) const
{
    std::unordered_map<String, Strings> partitions_by_replica;

    /// TODO: Intersect assignment_map with queried partition IDs to reduce routing overhead.
    /// When a query has a partition predicate (e.g. WHERE event_date = '2024-01-15'), only
    /// partitions matching the predicate need to be routed. Use storage.getAllPartitionIds() intersection
    /// or KeyCondition-based pruning to eliminate unnecessary remote connections and reduce the
    /// size of _partition_id IN (...) filter clauses. See cr_selective_replication.md M2.
    for (const auto & [partition_id, assigned_replicas] : assignment_map)
    {
        if (pid_restriction && !pid_restriction->contains(partition_id))
            continue;

        if (assigned_replicas.empty())
        {
            partitions_by_replica[storage.replica_name].push_back(partition_id);
            continue;
        }

        /// Prefer local replica; otherwise pick by hash-based load balancing.
        if (KeeperReplicaAssignment::isReplicaAssigned(assigned_replicas, storage.replica_name))
        {
            partitions_by_replica[storage.replica_name].push_back(partition_id);
        }
        else
        {
            Strings clean_replicas;
            clean_replicas.reserve(assigned_replicas.size());
            for (const auto & r : assigned_replicas)
                clean_replicas.push_back(KeeperReplicaAssignment::stripCloningSuffix(r));

            size_t idx = std::hash<String>{}(partition_id) % clean_replicas.size();
            partitions_by_replica[clean_replicas[idx]].push_back(partition_id);
        }
    }

    LOG_DEBUG(log, "Selective routing: partitions_by_replica for storage.replica_name={}", storage.replica_name);
    for (const auto & [rep, pids] : partitions_by_replica)
        LOG_DEBUG(log, "  {} -> [{}]", rep, fmt::join(pids, ","));

    /// Also include local partitions absent from the assignment map (pre-existing or fallback writes).
    /// When pid_restriction is set, restrict to intersection with that set.
    auto all_local_pids = storage.getAllPartitionIds();
    local_partition_ids_set = {all_local_pids.begin(), all_local_pids.end()};

    for (const auto & pid : local_partition_ids_set)
    {
        if (pid_restriction && !pid_restriction->contains(pid))
            continue;
        if (!assignment_map.contains(pid))
            partitions_by_replica[storage.replica_name].push_back(pid);
    }

    return partitions_by_replica;
}


void Router::redistributeFailedReplicas(
    std::unordered_map<String, Strings> & partitions_by_replica,
    const std::unordered_map<String, ReplicatedMergeTreeAddress> & address_map,
    const std::unordered_map<String, Strings> & assignment_map,
    const std::unordered_set<String> & local_partition_ids_set,
    const Settings & settings) const
{
    std::unordered_set<String> failed_replicas;
    for (const auto & [rep, _] : partitions_by_replica)
    {
        if (rep != storage.replica_name && !address_map.contains(rep))
            failed_replicas.insert(rep);
    }

    if (failed_replicas.empty())
        return;

    Strings lost_partitions;
    std::map<String, Strings> redistribution_plan;

    for (const auto & failed_replica : failed_replicas)
    {
        const auto & pids = partitions_by_replica.at(failed_replica);
        LOG_WARNING(log, "Selective replication: host info for replica {} not available "
            "in ZK, redistributing {} partition(s) to other replicas", failed_replica, pids.size());

        for (const auto & pid : pids)
        {
            bool reassigned = false;
            auto map_it = assignment_map.find(pid);
            if (map_it != assignment_map.end())
            {
                for (const auto & r : map_it->second)
                {
                    auto clean_r = KeeperReplicaAssignment::stripCloningSuffix(r);
                    if (clean_r != failed_replica && address_map.contains(clean_r))
                    {
                        redistribution_plan[clean_r].push_back(pid);
                        reassigned = true;
                        LOG_DEBUG(log, "Selective replication: redistributed partition {} "
                            "from {} to {}", pid, failed_replica, clean_r);
                        break;
                    }
                }
            }

            if (!reassigned)
            {
                if (local_partition_ids_set.contains(pid))
                {
                    LOG_WARNING(log, "Selective replication: no remote replica available "
                        "for partition {}, falling back to local read", pid);
                    redistribution_plan[storage.replica_name].push_back(pid);
                }
                else
                {
                    lost_partitions.push_back(pid);
                    LOG_ERROR(log, "Selective replication: partition {} is lost - "
                        "no reachable assigned replica and not available locally", pid);
                }
            }
        }
    }

    /// Apply redistribution plan.
    for (const auto & [target_replica, new_partitions] : redistribution_plan)
    {
        auto & target_list = partitions_by_replica[target_replica];
        target_list.insert(target_list.end(), new_partitions.begin(), new_partitions.end());
    }
    for (const auto & rep : failed_replicas)
        partitions_by_replica.erase(rep);

    /// Handle lost partitions.
    if (!lost_partitions.empty())
    {
        String pid_list = fmt::format("{}", fmt::join(
            lost_partitions.begin(),
            lost_partitions.begin() + std::min(lost_partitions.size(), size_t(5)),
            ", "));
        if (lost_partitions.size() > 5)
            pid_list += fmt::format(", ... ({} total)", lost_partitions.size());

        if (settings[Setting::skip_unavailable_shards])
        {
            LOG_WARNING(log, "Selective replication: {} partition(s) have no reachable "
                "assigned replica and no local data, skipping (skip_unavailable_shards=1): {}",
                lost_partitions.size(), pid_list);
        }
        else
        {
            throw Exception(ErrorCodes::ALL_REPLICAS_LOST,
                "Selective replication: {} partition(s) have no reachable assigned replica "
                "and no local data. Set skip_unavailable_shards=1 to skip them. "
                "Lost partitions: {}",
                lost_partitions.size(), pid_list);
        }
    }
}


std::unordered_set<String> Router::extractPartitionIdsFromQueryInfo(
    const SelectQueryInfo & query_info,
    ContextPtr local_context) const
{
    /// Walk a QueryTree WHERE subtree and collect partition-id literals from any
    /// `in(_partition_id, ...)` or `equals(_partition_id, literal)` predicate.
    /// Top-level `and(...)` is recursed into; anything else is ignored. This is
    /// a read-only inspection — it does NOT mutate the planner_context or the
    /// query_info, so it is safe to call at any depth.
    std::function<void(const QueryTreeNodePtr &, std::unordered_set<String> &)> collect_from_query_tree;
    collect_from_query_tree = [&](const QueryTreeNodePtr & node, std::unordered_set<String> & out)
    {
        if (!node)
            return;
        const auto * func = node->as<FunctionNode>();
        if (!func)
            return;
        const auto & name = func->getFunctionName();
        const auto & args = func->getArguments().getNodes();

        if (name == "and" || name == "or")
        {
            if (name == "and")
            {
                for (const auto & arg : args)
                    collect_from_query_tree(arg, out);
            }
            else
            {
                /// For OR, we can safely extract partition IDs only when EVERY branch
                /// produces at least one partition ID. If any branch yields nothing
                /// (e.g., it contains a non-partition-id predicate), that branch could
                /// match ALL partitions, making a union of the other branches unsound.
                std::unordered_set<String> or_result;
                bool all_branches_have_pids = true;
                for (const auto & arg : args)
                {
                    std::unordered_set<String> branch_pids;
                    collect_from_query_tree(arg, branch_pids);
                    if (branch_pids.empty())
                    {
                        all_branches_have_pids = false;
                        break;
                    }
                    or_result.insert(branch_pids.begin(), branch_pids.end());
                }
                if (all_branches_have_pids && !or_result.empty())
                    out.insert(or_result.begin(), or_result.end());
            }
            return;
        }

        if ((name == "in" || name == "equals") && args.size() == 2)
        {
            const auto * column = args[0]->as<ColumnNode>();
            if (!column || column->getColumnName() != "_partition_id")
                return;
            const auto * literal = args[1]->as<ConstantNode>();
            if (!literal)
                return;
            const Field & value = literal->getValue();
            if (value.getType() == Field::Types::Array)
            {
                for (const Field & item : value.safeGet<Array>())
                    out.insert(item.safeGet<String>());
            }
            else if (value.getType() == Field::Types::Tuple)
            {
                for (const Field & item : value.safeGet<Tuple>())
                    out.insert(item.safeGet<String>());
            }
            else if (value.getType() == Field::Types::String)
            {
                out.insert(value.safeGet<String>());
            }
        }
    };

    /// Walk a raw AST subtree. Structurally identical to the QueryTree walker
    /// but uses ASTFunction/ASTLiteral/ASTIdentifier — suitable for AST coming
    /// from `parseAdditionalTableFilterAST` (which returns an un-analyzed AST).
    std::function<void(const ASTPtr &, std::unordered_set<String> &)> collect_from_ast;
    collect_from_ast = [&](const ASTPtr & node, std::unordered_set<String> & out)
    {
        if (!node)
            return;
        const auto * func = node->as<ASTFunction>();
        if (!func)
            return;
        const String & name = func->name;

        if (name == "and" || name == "or")
        {
            if (name == "and" && func->arguments)
            {
                for (const auto & child : func->arguments->children)
                    collect_from_ast(child, out);
            }
            else if (name == "or" && func->arguments)
            {
                /// Same logic as in collect_from_query_tree: take the union
                /// of all branches only when every branch yields partition IDs.
                std::unordered_set<String> or_result;
                bool all_branches_have_pids = true;
                for (const auto & child : func->arguments->children)
                {
                    std::unordered_set<String> branch_pids;
                    collect_from_ast(child, branch_pids);
                    if (branch_pids.empty())
                    {
                        all_branches_have_pids = false;
                        break;
                    }
                    or_result.insert(branch_pids.begin(), branch_pids.end());
                }
                if (all_branches_have_pids && !or_result.empty())
                    out.insert(or_result.begin(), or_result.end());
            }
            return;
        }

        if ((name == "in" || name == "equals") && func->arguments && func->arguments->children.size() == 2)
        {
            const auto * ident = func->arguments->children[0]->as<ASTIdentifier>();
            if (!ident || ident->name() != "_partition_id")
                return;
            const auto * literal = func->arguments->children[1]->as<ASTLiteral>();
            if (!literal)
                return;
            const Field & value = literal->value;
            if (value.getType() == Field::Types::Array)
            {
                for (const Field & item : value.safeGet<Array>())
                    out.insert(item.safeGet<String>());
            }
            else if (value.getType() == Field::Types::Tuple)
            {
                for (const Field & item : value.safeGet<Tuple>())
                    out.insert(item.safeGet<String>());
            }
            else if (value.getType() == Field::Types::String)
            {
                out.insert(value.safeGet<String>());
            }
        }
    };

    /// Path 1: local shard — `additional_table_filters` carries the partition filter.
    /// Parse it into an AST with the shared `parseAdditionalTableFilterAST` helper and
    /// walk the AST directly (no Planner involvement → zero side effects).
    {
        auto table_alias = query_info.table_expression ? query_info.table_expression->getAlias() : String{};
        if (auto filter_ast = parseAdditionalTableFilterAST(storage.getStorageID(), table_alias, local_context))
        {
            std::unordered_set<String> pids;
            collect_from_ast(filter_ast, pids);
            if (!pids.empty())
                return pids;
        }
    }

    /// Path 2: remote shard — the upstream router injected `_partition_id IN (...)` into
    /// the SQL WHERE clause via `shard_filter`/`applyFilterToQuery`. Walk the WHERE
    /// subtree of `query_info.query_tree` read-only.
    if (query_info.query_tree)
    {
        if (const auto * query_node = query_info.query_tree->as<QueryNode>();
            query_node && query_node->hasWhere())
        {
            std::unordered_set<String> pids;
            collect_from_query_tree(query_node->getWhere(), pids);
            if (!pids.empty())
                return pids;
        }
    }

    /// Path 3: fallback — use `query_info.filter_actions_dag` if populated. Retained
    /// for completeness; unlikely to contribute in the selective replication code paths since paths 1/2
    /// already cover local + remote injection sources.
    if (query_info.filter_actions_dag)
    {
        const auto & outputs = query_info.filter_actions_dag->getOutputs();
        if (outputs.empty())
            return {};
        const ActionsDAG::Node * predicate = outputs.front();

        Block allowed_inputs;
        allowed_inputs.insert(ColumnWithTypeAndName(
            DataTypeString().createColumn(),
            std::make_shared<DataTypeString>(),
            "_partition_id"));

        auto extracted_dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(
            predicate, &allowed_inputs, local_context, /*allow_partial_result=*/true);
        if (!extracted_dag)
            return {};

        bool has_partition_id_input = false;
        for (const auto & input : extracted_dag->getInputs())
        {
            if (input->result_name == "_partition_id")
            {
                has_partition_id_input = true;
                break;
            }
        }
        if (!has_partition_id_input)
            return {};

        auto all_pids = storage.getAllPartitionIds();
        if (all_pids.empty())
            return {};

        MutableColumnPtr pid_col = DataTypeString().createColumn();
        pid_col->reserve(all_pids.size());
        for (const auto & pid : all_pids)
            pid_col->insert(pid);

        Block block;
        block.insert(ColumnWithTypeAndName(
            std::move(pid_col),
            std::make_shared<DataTypeString>(),
            "_partition_id"));

        VirtualColumnUtils::buildSetsForDAG(*extracted_dag, local_context);
        auto actions = VirtualColumnUtils::buildFilterExpression(std::move(*extracted_dag), local_context);
        VirtualColumnUtils::filterBlockWithExpression(actions, block);

        return VirtualColumnUtils::extractSingleValueFromBlock<String>(block, "_partition_id");
    }

    return {};
}


bool Router::hasMisplacedReceivedPartitions(
    const SelectQueryInfo & query_info,
    ContextPtr local_context) const
{
    /// Must mirror `reEnterSelectiveRoutingAtDepth` Steps 1-4b: extract received pids
    /// from the query filter, then for each pid consult the in-memory cache. A pid is
    /// misplaced only if the cache has an entry AND that entry does not include
    /// `storage.replica_name`. Cache-miss pids count as owned (they fall through to local read
    /// in the actual execution path). Self-loop candidates (targets whose address
    /// resolves to our own host:tcp_port) are also treated as owned to avoid infinite
    /// forwarding when multiple replicas live in the same process (single-server tests).
    auto received_pids = extractPartitionIdsFromQueryInfo(query_info, local_context);
    if (received_pids.empty())
        return false;

    std::vector<String> received_pids_vec(received_pids.begin(), received_pids.end());
    auto sub_assignments = storage.replica_assignment->getAssignments(
        /*zk=*/ nullptr, received_pids_vec, /*force_refresh=*/ false);

    /// First pass: collect candidate remote replicas we'd forward to, so we can
    /// resolve their addresses once.
    std::unordered_set<String> candidate_set;
    for (const auto & pid : received_pids)
    {
        auto it = sub_assignments.find(pid);
        if (it == sub_assignments.end() || it->second.replicas.empty())
            continue;
        if (KeeperReplicaAssignment::isReplicaAssigned(it->second.replicas, storage.replica_name))
            continue;
        for (const auto & r : it->second.replicas)
        {
            auto clean = KeeperReplicaAssignment::stripCloningSuffix(r);
            if (clean != storage.replica_name)
                candidate_set.insert(clean);
        }
    }

    std::unordered_set<String> self_loop_replicas;
    if (!candidate_set.empty())
    {
        Strings candidate_replicas(candidate_set.begin(), candidate_set.end());
        auto zk_for_lookup = storage.tryGetZooKeeper();
        if (zk_for_lookup)
        {
            /// `hasMisplacedReceivedPartitions` runs inside `getQueryProcessingStage`,
            /// which is called from the Planner on a thread where Coordination's
            /// current component has not been set. Set one explicitly so the
            /// underlying ZK multiRead call in `getReplicaAddresses` does not trip
            /// the `Current component is empty` LOGICAL_ERROR assertion.
            auto component_guard = Coordination::setCurrentComponent(
                "SelectiveReplication::Router::hasMisplacedReceivedPartitions");
            try
            {
                auto addr_map = getReplicaAddressesCached(candidate_replicas);
                const UInt16 my_tcp_port = storage.getContext()->getTCPPort();
                for (const auto & [rep, addr] : addr_map)
                {
                    try
                    {
                        if (isLocalAddress(Poco::Net::SocketAddress(addr.host, addr.queries_port), my_tcp_port))
                            self_loop_replicas.insert(rep);
                    }
                    catch (const Poco::Exception &)
                    {
                        /// DNS or address resolution failure — skip this candidate.
                        tryLogCurrentException(log, fmt::format("Failed to resolve address for replica '{}' during self-loop check", rep), LogsLevel::warning);
                    }
                }
            }
            catch (const DB::Exception &)
            {
                /// ZK error during address lookup — cannot detect self-loop, conservative behavior.
                tryLogCurrentException(log, "ZK error during self-loop check in hasMisplacedReceivedPartitions", LogsLevel::warning);
            }
        }
    }

    for (const auto & pid : received_pids)
    {
        auto it = sub_assignments.find(pid);
        if (it == sub_assignments.end() || it->second.replicas.empty())
            continue;
        if (KeeperReplicaAssignment::isReplicaAssigned(it->second.replicas, storage.replica_name))
            continue;

        /// Check if every candidate owner is a self-loop. If so, this pid is not
        /// actually misplaced from a forwarding perspective.
        bool all_self_loop = !self_loop_replicas.empty();
        for (const auto & r : it->second.replicas)
        {
            auto clean = KeeperReplicaAssignment::stripCloningSuffix(r);
            if (clean != storage.replica_name && !self_loop_replicas.contains(clean))
            {
                all_self_loop = false;
                break;
            }
        }
        if (!all_self_loop)
            return true;
    }
    return false;
}


std::unordered_set<String> Router::detectSelfLoopReplicas(
    const std::unordered_set<String> & candidate_replicas) const
{
    std::unordered_set<String> self_loop_replicas;
    if (candidate_replicas.empty())
        return self_loop_replicas;

    Strings candidate_vec(candidate_replicas.begin(), candidate_replicas.end());
    try
    {
        auto zk_for_lookup = storage.getZooKeeper();
        auto addr_map = getReplicaAddresses(candidate_vec, zk_for_lookup);
        const UInt16 my_tcp_port = storage.getContext()->getTCPPort();
        for (const auto & [rep, addr] : addr_map)
        {
            try
            {
                if (isLocalAddress(Poco::Net::SocketAddress(addr.host, addr.queries_port), my_tcp_port))
                    self_loop_replicas.insert(rep);
            }
            catch (const Poco::Exception & e)
            {
                /// DNS failure etc. — skip; the normal path will surface the error.
                LOG_WARNING(log, "Failed to resolve address for replica {}: {}", rep, e.displayText());
            }
        }
    }
    catch (const Exception & e)
    {
        /// ZK unavailable — we cannot detect self-loop, conservative behavior.
        LOG_WARNING(log, "ZooKeeper error while detecting self-loop replicas: {}", e.message());
    }
    return self_loop_replicas;
}

Strings Router::getAllReplicaNames() const
{
    auto zookeeper = storage.getZooKeeper();
    Strings replicas = zookeeper->getChildren(fs::path(storage.zookeeper_path) / "replicas");
    std::sort(replicas.begin(), replicas.end());
    return replicas;
}

Strings Router::getActiveReplicaNames(const zkutil::ZooKeeperPtr & zk) const
{
    Strings all = zk->getChildren(fs::path(storage.zookeeper_path) / "replicas");
    Strings active;
    active.reserve(all.size());
    for (const auto & replica : all)
    {
        if (zk->exists(fs::path(storage.zookeeper_path) / "replicas" / replica / "is_active"))
            active.push_back(replica);
    }
    std::sort(active.begin(), active.end());
    return active;
}

std::unordered_map<String, ReplicatedMergeTreeAddress> Router::getReplicaAddresses(
    const Strings & replicas, const zkutil::ZooKeeperPtr & zk) const
{
    std::unordered_map<String, ReplicatedMergeTreeAddress> result;
    if (replicas.empty())
        return result;

    Strings paths;
    paths.reserve(replicas.size());
    for (const auto & replica : replicas)
        paths.push_back(fs::path(storage.zookeeper_path) / "replicas" / replica / "host");

    auto responses = zk->tryGet(paths);
    for (size_t i = 0; i < replicas.size(); ++i)
    {
        if (responses[i].error != Coordination::Error::ZOK)
        {
            LOG_WARNING(log, "Replica {} address not found in ZK (path {}). "
                "The replica may be newly initialized or offline. "
                "Check the replica status and ensure it has completed startup.",
                replicas[i], paths[i]);
            continue;
        }
        result[replicas[i]] = ReplicatedMergeTreeAddress(responses[i].data);
    }

    return result;
}

void Router::enrichSnapshotWithAssignments(StorageSnapshotPtr & snapshot) const
{
    auto component_guard = Coordination::setCurrentComponent("SelectiveReplication::Router::enrichSnapshotWithAssignments");
    auto * snapshot_data = assert_cast<StorageReplicatedMergeTree::SnapshotData *>(snapshot->data.get());

    auto zk = storage.getZooKeeper();
    auto assignments = storage.replica_assignment->getAssignments(zk, {});

    std::unordered_map<String, Strings> result;
    for (const auto & [pid, entry] : assignments)
        result[pid] = entry.replicas;

    snapshot_data->selective_assignment_map = std::move(result);
}

Router::ReplacePartitionCASGuard Router::buildReplacePartitionCASGuard(
    const zkutil::ZooKeeperPtr & zk,
    const String & partition_id) const
{
    ReplacePartitionCASGuard guard;

    auto assign_map = storage.replica_assignment->getAssignments(zk, {partition_id}, /*force_refresh=*/true);
    auto assign_it = assign_map.find(partition_id);
    KeeperReplicaAssignment::CachedEntry assignment = assign_it != assign_map.end()
        ? assign_it->second : KeeperReplicaAssignment::CachedEntry{};

    if (assignment.version >= 0)
    {
        guard.assignment_cas_version = assignment.version;
        guard.this_replica_is_assigned = KeeperReplicaAssignment::isReplicaAssigned(
            assignment.replicas, storage.replica_name);

        if (!guard.this_replica_is_assigned)
        {
            LOG_INFO(log, "Selective replication: replica {} is NOT assigned to partition {} (assigned: [{}], version: {}). "
                "Will create log entry but skip local data commit.",
                storage.replica_name, partition_id, fmt::join(assignment.replicas, ","), assignment.version);
        }
    }
    else
    {
        throw Exception(ErrorCodes::ALL_REPLICAS_ARE_STALE,
            "Selective replication: assignment node for partition {} disappeared after "
            "allocatePartitions during REPLACE PARTITION, cannot proceed safely",
            partition_id);
    }

    return guard;
}

std::shared_ptr<StorageReplicatedMergeTree> Router::findSiblingStorage(const String & target_replica_name) const
{
    const auto & catalog = DatabaseCatalog::instance();
    for (const auto & [_, db] : catalog.getDatabases({}))
    {
        for (auto it = db->getTablesIterator(storage.getContext()); it->isValid(); it->next())
        {
            auto table = it->table();
            auto * candidate = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
            if (candidate
                && candidate != &storage
                && candidate->zookeeper_path == storage.zookeeper_path
                && candidate->getReplicaName() == target_replica_name)
            {
                return std::shared_ptr<StorageReplicatedMergeTree>(table, candidate);
            }
        }
    }
    return nullptr;
}

}
}
