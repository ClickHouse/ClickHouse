#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <mutex>
#include <unordered_set>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageTimeSeries.h>


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
}

namespace
{
    /// Keyed on cluster, remote table and the fleet's own addresses, so a reload that moves, adds
    /// or renames a shard yields a different key and the stale verdict is never found again.
    std::mutex validated_shard_engines_mutex;
    std::unordered_set<String> validated_shard_engines TSA_GUARDED_BY(validated_shard_engines_mutex);
}

void checkPrometheusQueryDistributedWrite(const IStorage & storage, const ContextPtr & context)
{
    auto target = resolvePrometheusQueryTarget(storage);
    if (!target)
        return;

    const auto & remote_id = target->remote_time_series_storage_id;
    const auto & distributed = typeid_cast<const StorageDistributed &>(storage);

    /// Raw fields: the remote database is legitimately empty when shards use their own defaults.
    WriteBufferFromOwnString key_buf;
    writeStringBinary(target->cluster_name, key_buf);
    writeStringBinary(remote_id.database_name, key_buf);
    writeStringBinary(remote_id.table_name, key_buf);
    for (const auto & shard : distributed.getCluster()->getShardsAddresses())
        for (const auto & replica : shard)
        {
            writeStringBinary(replica.host_name, key_buf);
            writeIntBinary(replica.port, key_buf);
            writeStringBinary(replica.default_database, key_buf);
        }
    const auto key = key_buf.str();

    {
        std::lock_guard lock(validated_shard_engines_mutex);
        if (validated_shard_engines.contains(key))
            return;
    }

    /// `view()` leaves the body unanalysed on the initiator, so an undeclared remote database
    /// resolves to each shard's own default; bare `currentDatabase()` would fold to this node's.
    const String database_predicate = remote_id.database_name.empty()
        ? "currentDatabase()"
        : quoteString(remote_id.database_name);
    const String probe_query = "SELECT countIf(engine != 'TimeSeries') FROM cluster("
        + backQuoteIfNeed(target->cluster_name) + ", view(SELECT engine FROM system.tables WHERE database = "
        + database_predicate + " AND name = " + quoteString(remote_id.table_name) + "))";

    /// On the server's own context, and only ever after the caller's INSERT check: the caller needs
    /// no cluster grant of its own, and learns nothing it could not already see.
    auto probe_context = Context::createCopy(context->getGlobalContext());
    probe_context->makeQueryContext();
    probe_context->setCurrentQueryId("");
    probe_context->setSetting("skip_unavailable_shards", false);

    auto [probe_ast, probe_io] = executeQuery(probe_query, probe_context, QueryFlags{ .internal = true });
    PullingPipelineExecutor executor(probe_io.pipeline);
    UInt64 wrong_engine_shards = 0;
    Block block;
    while (executor.pull(block))
        if (block.rows())
            wrong_engine_shards = block.getByPosition(0).column->getUInt(0);

    if (wrong_engine_shards)
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation is not supported over table {}: {} shard-local target(s) named {} are not "
            "TimeSeries tables, so samples written there could not be read back by any prometheus surface",
            storage.getStorageID().getNameForLogs(), wrong_engine_shards, backQuoteIfNeed(remote_id.table_name));

    std::lock_guard lock(validated_shard_engines_mutex);
    validated_shard_engines.insert(key);
}

std::pair<bool, String> declaredShardSkipSettings(const IStorage & storage, const ContextPtr & context)
{
    /// Handed to the generated cluster() call as its own declaration, so ClusterProxy applies the
    /// query-overrides-declaration rule itself rather than this file restating it.
    DistributedSettings declared;
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    if (const auto & settings_changes = metadata->settings_changes)
        declared.applyChanges(settings_changes->as<const ASTSetQuery &>().changes);
    return {declared[DistributedSetting::skip_unavailable_shards].value,
            declared[DistributedSetting::skip_unavailable_shards_mode].toString()};
}

}
