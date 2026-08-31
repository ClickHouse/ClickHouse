#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
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
    extern const SettingsMap additional_table_filters;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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

void checkTimeSeriesWrapperReadContract(const StorageID & storage_id, const ContextPtr & context)
{
    auto row_policy_filter = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A row policy on {} is not supported here: the read is rewritten to inner or shard "
            "TimeSeries tables and the policy would not be applied",
            storage_id.getNameForLogs());

    /// Matched exactly the way the planner matches filter keys (PlannerJoinTree): the short name
    /// only from the same current database, the full unquoted name from anywhere.
    for (const auto & filter_entry : context->getSettingsRef()[Setting::additional_table_filters].value)
    {
        const auto & name_and_filter = filter_entry.safeGet<Tuple>();
        const auto & filtered_table = name_and_filter.at(0).safeGet<String>();
        bool matches = (filtered_table == storage_id.getTableName()
                        && context->getCurrentDatabase() == storage_id.getDatabaseName())
            || filtered_table == storage_id.getFullNameNotQuoted();
        if (matches && !name_and_filter.at(1).safeGet<String>().empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "An additional_table_filters entry for {} is not supported here: the read is rewritten "
                "to inner or shard TimeSeries tables and the filter would not be applied",
                storage_id.getNameForLogs());
    }
}

void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context)
{
    /// The rewrite drops the wrapper before the planner ever sees it, so its access
    /// is enforced here or not at all.
    auto storage_id = storage.getStorageID();
    context->checkAccess(AccessType::SELECT, storage_id);
    checkTimeSeriesWrapperReadContract(storage_id, context);

    /// The generated cluster() call cannot carry the wrapper's declared Distributed settings,
    /// so a read that would diverge from a plain SELECT is refused rather than silently changed.
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    if (const auto & settings_changes = metadata->settings_changes)
    {
        /// Mirrors ClusterProxy::executeQuery: declarations are defaults the query overrides,
        /// so the reads diverge only when the query leaves a setting unset and the declaration differs.
        DistributedSettings declared;
        declared.applyChanges(settings_changes->as<const ASTSetQuery &>().changes);
        const auto & query_settings = context->getSettingsRef();
        const bool skip_diverges = !query_settings[Setting::skip_unavailable_shards].changed
            && declared[DistributedSetting::skip_unavailable_shards].value != query_settings[Setting::skip_unavailable_shards].value;
        /// The mode controls which exceptions shard skipping ignores, so with skipping effectively
        /// off it cannot change behavior; compare it only under the normal path's effective flag.
        const bool effective_skip = query_settings[Setting::skip_unavailable_shards].changed
            ? query_settings[Setting::skip_unavailable_shards].value
            : declared[DistributedSetting::skip_unavailable_shards].value;
        const bool mode_diverges = effective_skip
            && !query_settings[Setting::skip_unavailable_shards_mode].changed
            && declared[DistributedSetting::skip_unavailable_shards_mode].value != query_settings[Setting::skip_unavailable_shards_mode].value;
        if (skip_diverges || mode_diverges)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "This operation is not supported over table {} because it sets skip_unavailable_shards or "
                "skip_unavailable_shards_mode to a non-default value: a prometheus query reads through a "
                "generated cluster() call, which does not carry the table's Distributed settings",
                storage_id.getNameForLogs());
    }
}

}
