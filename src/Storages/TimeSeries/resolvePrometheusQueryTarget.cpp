#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>
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

void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context)
{
    /// The read is rewritten to cluster(view(timeSeriesSelector(...))) over the shards' TimeSeries
    /// tables and the planner never sees the wrapper again, so its access is enforced here or not
    /// at all.
    auto storage_id = storage.getStorageID();
    context->checkAccess(AccessType::SELECT, storage_id);
    auto row_policy_filter = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A row policy on {} is not supported for prometheus queries over a Distributed table: "
            "the read is rewritten to the shards' TimeSeries tables and the policy would not be applied",
            storage_id.getNameForLogs());

    /// The generated cluster() call cannot carry the wrapper's DistributedSettings, so a wrapper
    /// declaring non-default shard-skipping would silently behave differently through a prometheus
    /// query than through a plain SELECT. Refuse rather than diverge; the query-level
    /// `skip_unavailable_shards` setting still applies to the generated call. Read from the
    /// metadata's declared changes: the concern is what the table declares, and the effective
    /// settings are private to StorageDistributed.
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    if (const auto & settings_changes = metadata->settings_changes)
    {
        /// Compared by value, not presence: declaring the default explicitly changes nothing about
        /// the read and must not refuse it.
        DistributedSettings declared;
        declared.applyChanges(settings_changes->as<const ASTSetQuery &>().changes);
        const DistributedSettings defaults;
        if (declared[DistributedSetting::skip_unavailable_shards].value != defaults[DistributedSetting::skip_unavailable_shards].value
            || declared[DistributedSetting::skip_unavailable_shards_mode].value != defaults[DistributedSetting::skip_unavailable_shards_mode].value)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "This operation is not supported over table {} because it sets skip_unavailable_shards or "
                "skip_unavailable_shards_mode to a non-default value: a prometheus query reads through a "
                "generated cluster() call, which does not carry the table's Distributed settings",
                storage_id.getNameForLogs());
    }
}

}
