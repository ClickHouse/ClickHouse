#include <Interpreters/ClusterMetadataQueryStatusSource.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
}

namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
}

ClusterMetadataQueryStatusSource::ClusterMetadataQueryStatusSource(
    const String & zookeeper_name_,
    const String & zk_node_path,
    const String & zk_replicas_path,
    ContextPtr context_,
    const Strings & hosts_to_wait)
    : DistributedQueryStatusSource(
          zookeeper_name_,
          zk_node_path,
          zk_replicas_path,
          std::make_shared<const Block>(getSampleBlock(context_->getSettingsRef()[Setting::distributed_ddl_output_mode])),
          context_,
          hosts_to_wait,
          "ClusterMetadataQueryStatusSource")
    , output_mode(context_->getSettingsRef()[Setting::distributed_ddl_output_mode])
{
    loadReplicaAddresses();
}

void ClusterMetadataQueryStatusSource::loadReplicaAddresses()
{
    try
    {
        auto zookeeper = context->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
        for (const auto & replica_id : waiting_hosts)
        {
            String address;
            if (zookeeper->tryGet(fs::path(replicas_path) / replica_id, address) && !address.empty())
                replica_addresses.emplace(replica_id, std::move(address));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to load cluster metadata replica display addresses");
    }
}

String ClusterMetadataQueryStatusSource::formatReplicaLabel(const String & replica_id) const
{
    if (const auto it = replica_addresses.find(replica_id); it != replica_addresses.end() && !it->second.empty())
        return it->second;
    return replica_id;
}

std::pair<String, UInt16> ClusterMetadataQueryStatusSource::parseDisplayAddress(const String & label)
{
    if (label.find(':') == String::npos)
        return {label, 0};
    return Cluster::Address::fromString(label);
}

ExecutionStatus ClusterMetadataQueryStatusSource::checkStatus(const String & host_id)
{
    fs::path status_path = fs::path(node_path) / "finished" / host_id;
    return getExecutionStatus(status_path);
}

Chunk ClusterMetadataQueryStatusSource::generateChunkWithUnfinishedHosts() const
{
    NameSet unfinished_hosts = waiting_hosts;
    for (const auto & host_id : finished_hosts)
        unfinished_hosts.erase(host_id);

    MutableColumns columns = output.getHeader().cloneEmptyColumns();
    auto nullable_err_status = nullableErrorAndStatusFields(output_mode);
    for (const String & host_id : unfinished_hosts)
    {
        size_t num = 0;
        auto [host, port] = parseDisplayAddress(formatReplicaLabel(host_id));
        columns[num++]->insert(host);
        columns[num++]->insert(port);
        columns[num++]->insert(nullable_err_status ? Field{} : QueryStatus::UNFINISHED);
        columns[num++]->insert(nullable_err_status ? Field{} : "Unfinished");
        columns[num++]->insert(unfinished_hosts.size());
        columns[num++]->insert(current_active_hosts.size());
    }
    return Chunk(std::move(columns), unfinished_hosts.size());
}

Strings ClusterMetadataQueryStatusSource::getNodesToWait()
{
    return {String(fs::path(node_path) / "finished"), String(fs::path(node_path) / "active")};
}

Chunk ClusterMetadataQueryStatusSource::handleTimeoutExceeded()
{
    timeout_exceeded = true;

    size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
    size_t num_active_hosts = current_active_hosts.size();

    constexpr auto msg_format = "Cluster metadata DDL task {} is not finished on {} of {} nodes "
                                "({} of them are currently executing the task, {} are inactive). "
                                "They are going to apply the mutation in background. Was waiting for {} seconds{}";

    if (throw_on_timeout || (throw_on_timeout_only_active && !stop_waiting_offline_hosts))
    {
        if (!first_exception)
            first_exception = std::make_unique<Exception>(Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                msg_format,
                node_path,
                num_unfinished_hosts,
                waiting_hosts.size(),
                num_active_hosts,
                offline_hosts.size(),
                watch.elapsedSeconds(),
                stop_waiting_offline_hosts ? "" : ", which is longer than distributed_ddl_task_timeout"));

        return generateChunkWithUnfinishedHosts();
    }

    LOG_INFO(
        log,
        msg_format,
        node_path,
        num_unfinished_hosts,
        waiting_hosts.size(),
        num_active_hosts,
        offline_hosts.size(),
        watch.elapsedSeconds(),
        stop_waiting_offline_hosts ? "" : " which is longer than distributed_ddl_task_timeout");

    return generateChunkWithUnfinishedHosts();
}

Chunk ClusterMetadataQueryStatusSource::stopWaitingOfflineHosts()
{
    return handleTimeoutExceeded();
}

void ClusterMetadataQueryStatusSource::handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id)
{
    chassert(status.code != 0);

    if (!first_exception && context->getSettingsRef()[Setting::distributed_ddl_output_mode] != DistributedDDLOutputMode::NEVER_THROW)
    {
        first_exception = std::make_unique<Exception>(Exception(
            status.code,
            "There was an error on cluster metadata replica `{}`: {}",
            formatReplicaLabel(host_id),
            status.message));
    }
}

void ClusterMetadataQueryStatusSource::fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns)
{
    size_t num = 0;
    auto [host, port] = parseDisplayAddress(formatReplicaLabel(host_id));
    columns[num++]->insert(host);
    columns[num++]->insert(port);
    columns[num++]->insert(status.code);
    columns[num++]->insert(status.message);
    columns[num++]->insert(waiting_hosts.size() - num_hosts_finished);
    columns[num++]->insert(current_active_hosts.size());
}

bool ClusterMetadataQueryStatusSource::nullableErrorAndStatusFields(DistributedDDLOutputMode output_mode_)
{
    return !(
        output_mode_ == DistributedDDLOutputMode::THROW || ///
        output_mode_ == DistributedDDLOutputMode::NONE || ///
        output_mode_ == DistributedDDLOutputMode::NONE_ONLY_ACTIVE ///
    );
}

Block ClusterMetadataQueryStatusSource::getSampleBlock(DistributedDDLOutputMode output_mode_)
{
    auto nullable_error_status = nullableErrorAndStatusFields(output_mode_);
    auto maybe_make_nullable = [nullable_error_status](const DataTypePtr & type) -> DataTypePtr
    { return nullable_error_status ? std::make_shared<DataTypeNullable>(type) : type; };

    return Block{
        {std::make_shared<DataTypeString>(), "host"},
        {std::make_shared<DataTypeUInt16>(), "port"},
        {maybe_make_nullable(std::make_shared<DataTypeInt64>()), "status"},
        {maybe_make_nullable(std::make_shared<DataTypeString>()), "error"},
        {std::make_shared<DataTypeUInt64>(), "num_hosts_remaining"},
        {std::make_shared<DataTypeUInt64>(), "num_hosts_active"},
    };
}

}
