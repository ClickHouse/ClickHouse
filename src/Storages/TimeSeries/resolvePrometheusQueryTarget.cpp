#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Common/Exception.h>
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

std::pair<bool, String> effectiveShardSkipSemantics(const IStorage & storage, const ContextPtr & context)
{
    /// Mirrors ClusterProxy::executeQuery: declarations are defaults the query overrides. The
    /// result is pinned into the generated cluster() read, which replicates the wrapper's fan-out.
    DistributedSettings declared;
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    if (const auto & settings_changes = metadata->settings_changes)
        declared.applyChanges(settings_changes->as<const ASTSetQuery &>().changes);
    const auto & query_settings = context->getSettingsRef();
    const bool skip = query_settings[Setting::skip_unavailable_shards].changed
        ? query_settings[Setting::skip_unavailable_shards].value
        : declared[DistributedSetting::skip_unavailable_shards].value;
    const String mode = query_settings[Setting::skip_unavailable_shards_mode].changed
        ? query_settings[Setting::skip_unavailable_shards_mode].toString()
        : declared[DistributedSetting::skip_unavailable_shards_mode].toString();
    return {skip, mode};
}

}
