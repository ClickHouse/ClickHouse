#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Client/ConnectionPool.h>
#include <Columns/ColumnBLOB.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Cluster.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <chrono>
#include <mutex>
#include <set>
#include <unordered_map>
#include <fmt/ranges.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

namespace DistributedSetting
{
    extern const DistributedSettingsBool skip_unavailable_shards;
    extern const DistributedSettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_TABLE_ENGINE;
}

std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage)
{
    const auto * distributed = typeid_cast<const StorageDistributed *>(&storage);
    if (!distributed)
    {
        if (typeid_cast<const StorageTimeSeries *>(&storage))
            return {};

        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation can be executed on a TimeSeries table or on a Distributed table over TimeSeries tables only, "
            "the engine of table {} is not TimeSeries",
            storage.getStorageID().getNameForLogs());
    }

    PrometheusQueryDistributedTarget target;

    /// getClusterName() returns "<remote>" for ENGINE=Remote(), which is not a name we can pass to cluster().
    target.cluster_name = distributed->getClusterName();
    if (target.cluster_name == "<remote>")
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation is not supported over table {} because it has no cluster name: "
            "a prometheus query over a Distributed table requires a cluster defined in the server configuration",
            storage.getStorageID().getNameForLogs());

    auto remote_table_name = distributed->getRemoteTableName();
    if (remote_table_name.empty())
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation is not supported over table {} because it points to a table function: "
            "a prometheus query over a Distributed table requires a TimeSeries table on each shard",
            storage.getStorageID().getNameForLogs());

    target.remote_time_series_storage_id.database_name = distributed->getRemoteDatabaseName();
    target.remote_time_series_storage_id.table_name = std::move(remote_table_name);
    return target;
}

namespace
{
bool hasInstantSelector(const PrometheusQueryTree::Node * node)
{
    if (!node)
        return false;
    if (node->node_type == PrometheusQueryTree::NodeType::InstantSelector)
        return true;
    for (const auto * child : node->children)
        if (hasInstantSelector(child))
            return true;
    return false;
}
}

bool prometheusQueryReadsTimeSeries(const PrometheusQueryTree & promql_query)
{
    /// A range selector carries an instant selector as its child, so one node type covers both.
    return hasInstantSelector(promql_query.getRoot());
}

namespace
{
    /// Keyed on cluster, remote table, the wrapper's `time_series` type and the fleet's own
    /// addresses; a verdict also expires, so shard-side DDL under an unchanged key is re-probed.
    constexpr auto shard_targets_revalidation_period = std::chrono::minutes{1};
    std::mutex validated_shard_targets_mutex;
    std::unordered_map<String, std::chrono::steady_clock::time_point> validated_shard_targets
        TSA_GUARDED_BY(validated_shard_targets_mutex);

    /// Asks every replica itself, not one per shard as cluster() would: load balancing, failover or the
    /// sink may reach any of them later. An unreachable replica is skipped or refused as the caller says.
    void checkShardTargets(const IStorage & storage, const ContextPtr & context, bool refuse_unreachable)
    {
        auto target = resolvePrometheusQueryTarget(storage);
        if (!target)
            return;

        const auto & remote_id = target->remote_time_series_storage_id;
        const auto cluster = typeid_cast<const StorageDistributed &>(storage).getCluster();
        const auto metadata = storage.getInMemoryMetadataPtr(context, false);
        const auto time_series_type = metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type->getName();

        /// Raw fields: the remote database is legitimately empty when shards use their own defaults.
        WriteBufferFromOwnString key_buf;
        writeStringBinary(target->cluster_name, key_buf);
        writeStringBinary(remote_id.database_name, key_buf);
        writeStringBinary(remote_id.table_name, key_buf);
        writeStringBinary(time_series_type, key_buf);
        for (const auto & shard : cluster->getShardsAddresses())
            for (const auto & replica : shard)
            {
                writeStringBinary(replica.host_name, key_buf);
                writeIntBinary(replica.port, key_buf);
                writeStringBinary(replica.default_database, key_buf);
            }
        const auto key = key_buf.str();

        {
            std::lock_guard lock(validated_shard_targets_mutex);
            auto it = validated_shard_targets.find(key);
            if (it != validated_shard_targets.end()
                && std::chrono::steady_clock::now() - it->second < shard_targets_revalidation_period)
                return;
        }

        /// Sent as text over each replica's own connection, so an undeclared remote database resolves
        /// to that replica's configured default, exactly as the sink's and the rewrite's queries do.
        const String database_predicate = remote_id.database_name.empty()
            ? "currentDatabase()"
            : quoteString(remote_id.database_name);
        const String table_predicate = quoteString(remote_id.table_name);
        const String probe_query = "SELECT (SELECT engine FROM system.tables WHERE database = " + database_predicate
            + " AND name = " + table_predicate + ") AS engine, (SELECT type FROM system.columns WHERE database = "
            + database_predicate + " AND table = " + table_predicate + " AND name = " + quoteString(TimeSeriesColumnNames::TimeSeries)
            + ") AS ts_type";
        const auto nullable_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        const auto probe_header = std::make_shared<const Block>(Block{{nullable_string, "engine"}, {nullable_string, "ts_type"}});

        /// On the server's own context, and only ever after the caller's grant check: the caller needs
        /// no cluster grant of its own, and learns nothing it could not already see.
        auto probe_context = Context::createCopy(context->getGlobalContext());
        probe_context->makeQueryContext();
        probe_context->setCurrentQueryId("");
        /// An unreachable replica then answers nothing rather than failing the probe; judged below.
        probe_context->setSetting("skip_unavailable_shards", true);

        UInt64 wrong_engine_replicas = 0;
        UInt64 wrong_type_replicas = 0;
        std::set<String> wrong_types;
        Strings unreachable_replicas;
        for (const auto & shard : cluster->getShardsInfo())
            for (const auto & pool : shard.per_replica_pools)
            {
                RemoteQueryExecutor probe(pool, probe_query, probe_header, probe_context);
                bool answered = false;
                for (Block block = probe.readBlock(); !block.empty(); block = probe.readBlock())
                {
                    if (!block.rows())
                        continue;
                    block = convertBLOBColumns(block);
                    answered = true;
                    const Field engine = (*block.getByPosition(0).column)[0];
                    const Field ts_type = (*block.getByPosition(1).column)[0];
                    /// NULL where the table is absent, which is left to the read or the sink as for any INSERT.
                    if (engine.isNull())
                        continue;
                    if (engine.safeGet<String>() != "TimeSeries")
                        ++wrong_engine_replicas;
                    else if (!ts_type.isNull() && ts_type.safeGet<String>() != time_series_type)
                    {
                        ++wrong_type_replicas;
                        wrong_types.insert(ts_type.safeGet<String>());
                    }
                }
                if (!answered)
                    unreachable_replicas.push_back(pool->getAddress());
            }

        if (wrong_engine_replicas)
            throw Exception(
                ErrorCodes::UNEXPECTED_TABLE_ENGINE,
                "This operation is not supported over table {}: {} shard-local target(s) named {} are not TimeSeries tables",
                storage.getStorageID().getNameForLogs(), wrong_engine_replicas, backQuoteIfNeed(remote_id.table_name));

        if (wrong_type_replicas)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "This operation is not supported over table {}: {} shard-local target(s) named {} declare `{}` as {} "
                "while the table declares {}",
                storage.getStorageID().getNameForLogs(), wrong_type_replicas, backQuoteIfNeed(remote_id.table_name),
                TimeSeriesColumnNames::TimeSeries, fmt::join(wrong_types, ", "), time_series_type);

        if (!unreachable_replicas.empty())
        {
            /// Samples the sink queued for an unseen replica would be delivered later without any check.
            if (refuse_unreachable)
                throw Exception(
                    ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
                    "Remote write over table {} is refused while {} cannot be reached to verify its target: retry once it answers",
                    storage.getStorageID().getNameForLogs(), fmt::join(unreachable_replicas, ", "));

            /// A skipped replica may come back as anything: no verdict outlives it.
            return;
        }

        std::lock_guard lock(validated_shard_targets_mutex);
        validated_shard_targets[key] = std::chrono::steady_clock::now();
    }
}

void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context)
{
    /// The rewrite replaces the wrapper with a generated cluster() call before the planner sees
    /// it, so the wrapper's own SELECT grant is enforced here or not at all.
    context->checkAccess(AccessType::SELECT, storage.getStorageID());
    /// Whether an unreachable replica fails the read is the read's own decision, as for any cluster() call.
    checkShardTargets(storage, context, /* refuse_unreachable = */ false);
}

void checkPrometheusQueryDistributedWrite(const IStorage & storage, const ContextPtr & context)
{
    checkShardTargets(storage, context, /* refuse_unreachable = */ true);
}

std::pair<bool, String> declaredShardSkipSettings(const IStorage & storage)
{
    /// Handed to the generated cluster() call as its own declaration, so ClusterProxy applies the
    /// query-overrides-declaration rule itself rather than this file restating it.
    const auto & declared = typeid_cast<const StorageDistributed &>(storage).getDistributedSettingsRef();
    return {declared[DistributedSetting::skip_unavailable_shards].value,
            declared[DistributedSetting::skip_unavailable_shards_mode].toString()};
}

}
