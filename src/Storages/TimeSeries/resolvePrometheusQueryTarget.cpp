#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
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

namespace Setting
{
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace ErrorCodes
{
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

void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context)
{
    /// The rewrite replaces the wrapper with a generated cluster() call before the planner sees
    /// it, so the wrapper's own SELECT grant is enforced here or not at all.
    context->checkAccess(AccessType::SELECT, storage.getStorageID());
    checkPrometheusQueryDistributedTargets(storage, context);
}

namespace
{
    /// Keyed on cluster, remote table, the wrapper's `time_series` type and the fleet's own
    /// addresses; a verdict also expires, so shard-side DDL under an unchanged key is re-probed.
    constexpr auto shard_targets_revalidation_period = std::chrono::minutes{1};
    std::mutex validated_shard_targets_mutex;
    std::unordered_map<String, std::chrono::steady_clock::time_point> validated_shard_targets
        TSA_GUARDED_BY(validated_shard_targets_mutex);
}

void checkPrometheusQueryDistributedTargets(const IStorage & storage, const ContextPtr & context)
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

    /// `view()` leaves the body unanalysed on the initiator, so an undeclared remote database
    /// resolves to each shard's own default; bare `currentDatabase()` would fold to this node's.
    const String database_predicate = remote_id.database_name.empty()
        ? "currentDatabase()"
        : quoteString(remote_id.database_name);
    const String table_predicate = quoteString(remote_id.table_name);
    const String type_mismatch = "engine = 'TimeSeries' AND ts_type != " + quoteString(time_series_type);
    const auto [skip_unavailable_shards, skip_unavailable_shards_mode] = declaredShardSkipSettings(storage);

    /// Fans out like the read it guards: the wrapper's declared skip settings decide whether a dead
    /// shard is an error here too, and the query text is shipped for the same reason as there.
    const String probe_query = "SELECT count(), countIf(engine != 'TimeSeries'), countIf(" + type_mismatch
        + "), arrayStringConcat(groupUniqArrayIf(ts_type, " + type_mismatch + "), ', ') FROM cluster("
        + backQuoteIfNeed(target->cluster_name) + ", view(SELECT (SELECT engine FROM system.tables WHERE database = "
        + database_predicate + " AND name = " + table_predicate + ") AS engine, (SELECT type FROM system.columns WHERE database = "
        + database_predicate + " AND table = " + table_predicate + " AND name = " + quoteString(TimeSeriesColumnNames::TimeSeries)
        + ") AS ts_type), SETTINGS skip_unavailable_shards = " + String(skip_unavailable_shards ? "1" : "0")
        + ", skip_unavailable_shards_mode = " + quoteString(skip_unavailable_shards_mode) + ")";

    /// On the server's own context, and only ever after the caller's grant check: the caller needs
    /// no cluster grant of its own, and learns nothing it could not already see.
    auto probe_context = Context::createCopy(context->getGlobalContext());
    probe_context->makeQueryContext();
    probe_context->setCurrentQueryId("");
    probe_context->setSetting("serialize_query_plan", false);

    /// A query-level value overrides the declaration on the read as well (ClusterProxy's rule).
    const auto & caller_settings = context->getSettingsRef();
    if (caller_settings[Setting::skip_unavailable_shards].changed)
        probe_context->setSetting("skip_unavailable_shards", caller_settings[Setting::skip_unavailable_shards].value);
    if (caller_settings[Setting::skip_unavailable_shards_mode].changed)
        probe_context->setSetting("skip_unavailable_shards_mode", caller_settings[Setting::skip_unavailable_shards_mode].toString());

    auto [probe_ast, probe_io] = executeQuery(probe_query, probe_context, QueryFlags{ .internal = true });
    PullingPipelineExecutor executor(probe_io.pipeline);
    UInt64 answered_shards = 0;
    UInt64 wrong_engine_shards = 0;
    UInt64 wrong_type_shards = 0;
    String wrong_types;
    Block block;
    while (executor.pull(block))
        if (block.rows())
        {
            answered_shards = block.getByPosition(0).column->getUInt(0);
            wrong_engine_shards = block.getByPosition(1).column->getUInt(0);
            wrong_type_shards = block.getByPosition(2).column->getUInt(0);
            wrong_types = String(block.getByPosition(3).column->getDataAt(0));
        }

    if (wrong_engine_shards)
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation is not supported over table {}: {} shard-local target(s) named {} are not TimeSeries tables",
            storage.getStorageID().getNameForLogs(), wrong_engine_shards, backQuoteIfNeed(remote_id.table_name));

    if (wrong_type_shards)
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "This operation is not supported over table {}: {} shard-local target(s) named {} declare `{}` as {} "
            "while the table declares {}",
            storage.getStorageID().getNameForLogs(), wrong_type_shards, backQuoteIfNeed(remote_id.table_name),
            TimeSeriesColumnNames::TimeSeries, wrong_types, time_series_type);

    /// count() is the number of shards that answered, one row each (NULL scalars where the table is
    /// absent). A shard skipped as unavailable may come back as anything: no verdict outlives it.
    if (answered_shards != cluster->getShardCount())
        return;

    std::lock_guard lock(validated_shard_targets_mutex);
    validated_shard_targets[key] = std::chrono::steady_clock::now();
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
