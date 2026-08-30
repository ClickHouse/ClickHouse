#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

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

}
