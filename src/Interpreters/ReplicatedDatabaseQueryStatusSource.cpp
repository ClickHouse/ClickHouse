#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/ReplicatedDatabaseQueryStatusSource.h>
#include <Interpreters/Context.h>
#include <Common/FailPoint.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool database_replicated_enforce_synchronous_settings;
extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
}
namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int LOGICAL_ERROR;
}
namespace FailPoints
{
extern const char replicated_database_status_finished_node_missing[];
}

ReplicatedDatabaseQueryStatusSource::ReplicatedDatabaseQueryStatusSource(
    const String & zookeeper_name_,
    const String & zk_node_path,
    const String & zk_replicas_path,
    ContextPtr context_,
    const Strings & hosts_to_wait,
    DDLGuardPtr && database_guard_)
    : DistributedQueryStatusSource(
        zookeeper_name_,
        zk_node_path,
        zk_replicas_path,
        std::make_shared<const Block>(getSampleBlock()),
        context_,
        hosts_to_wait,
        "ReplicatedDatabaseQueryStatusSource")
    , database_guard(std::move(database_guard_))
{
}

ExecutionStatus ReplicatedDatabaseQueryStatusSource::checkStatus([[maybe_unused]] const String & host_id)
{
    /// A present finished/<host_id> for a Replicated database always holds status 0; an absent one is the benign
    /// cleaner/retry race, not a remote error. This is a debug/sanitizer-only cross-check.
#ifdef DEBUG_OR_SANITIZER_BUILD
    fs::path status_path = fs::path(node_path) / "finished" / host_id;
    bool node_exists = true;
    ExecutionStatus status = getExecutionStatus(status_path, &node_exists);
    /// Simulate the absent-node read (tryGet false -> node_exists false and the getExecutionStatus sentinel).
    fiu_do_on(FailPoints::replicated_database_status_finished_node_missing,
    {
        node_exists = false;
        status = ExecutionStatus(-1, "Cannot obtain error message");
    });
    /// Only the absent-node case is the benign race, so report success like the non-debug branch for it. A present
    /// node holds a real deserialized status (positive code), or the (-1) sentinel when its payload is corrupt
    /// (tryDeserializeText is atomic, so the sentinel survives) which the cross-check must still surface.
    if (!node_exists)
    {
        LOG_DEBUG(log, "finished node {} is absent (benign cleaner/retry race), reporting success", status_path.string());
        return ExecutionStatus{0};
    }
    if (status.code < 0)
        LOG_WARNING(log, "finished node {} is present but its status payload could not be deserialized, "
                    "surfacing it via the cross-check", status_path.string());
    return status;
#else
    return ExecutionStatus{0};
#endif
}

Chunk ReplicatedDatabaseQueryStatusSource::generateChunkWithUnfinishedHosts() const
{
    NameSet unfinished_hosts = waiting_hosts;
    for (const auto & host_id : finished_hosts)
        unfinished_hosts.erase(host_id);

    NameSet active_hosts_set = NameSet{current_active_hosts.begin(), current_active_hosts.end()};

    /// Query is not finished on the rest hosts, so fill the corresponding rows with NULLs.
    MutableColumns columns = output.getHeader().cloneEmptyColumns();
    for (const String & host_id : unfinished_hosts)
    {
        size_t num = 0;
        auto [shard, replica] = DatabaseReplicated::parseFullReplicaName(host_id);
        columns[num++]->insert(shard);
        columns[num++]->insert(replica);
        if (active_hosts_set.contains(host_id))
            columns[num++]->insert(QueryStatus::IN_PROGRESS);
        else
            columns[num++]->insert(QueryStatus::QUEUED);

        columns[num++]->insert(unfinished_hosts.size());
        columns[num++]->insert(current_active_hosts.size());
    }
    return Chunk(std::move(columns), unfinished_hosts.size());
}

Strings ReplicatedDatabaseQueryStatusSource::getNodesToWait()
{
    String node_to_wait = "finished";
    if (context->getSettingsRef()[Setting::database_replicated_enforce_synchronous_settings])
    {
        node_to_wait = "synced";
    }

    return {String(fs::path(node_path) / node_to_wait), String(fs::path(node_path) / "active")};
}

Chunk ReplicatedDatabaseQueryStatusSource::handleTimeoutExceeded()
{
    timeout_exceeded = true;

    size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
    size_t num_active_hosts = current_active_hosts.size();

    constexpr auto msg_format = "ReplicatedDatabase DDL task {} is not finished on {} of {} hosts "
                                "({} of them are currently executing the task, {} are inactive). "
                                "They are going to execute the query in background. Was waiting for {:.3f} seconds{}";

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

        /// For Replicated database print a list of unfinished hosts as well. Will return empty block on next iteration.
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
        stop_waiting_offline_hosts ? "" : "which is longer than distributed_ddl_task_timeout");

    return generateChunkWithUnfinishedHosts();
}

Chunk ReplicatedDatabaseQueryStatusSource::stopWaitingOfflineHosts()
{
    // Same logic as timeout exceeded
    return handleTimeoutExceeded();
}

void ReplicatedDatabaseQueryStatusSource::handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id)
{
    chassert(status.code != 0);

    if (!first_exception && context->getSettingsRef()[Setting::distributed_ddl_output_mode] != DistributedDDLOutputMode::NEVER_THROW)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There was an error on {}: {} (probably it's a bug)", host_id, status.message);
    }
}

void ReplicatedDatabaseQueryStatusSource::fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns)
{
    size_t num = 0;
    if (status.code != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There was an error on {}: {} (probably it's a bug)", host_id, status.message);
    auto [shard, replica] = DatabaseReplicated::parseFullReplicaName(host_id);
    columns[num++]->insert(shard);
    columns[num++]->insert(replica);
    columns[num++]->insert(QueryStatus::OK);
    columns[num++]->insert(waiting_hosts.size() - num_hosts_finished);
    columns[num++]->insert(current_active_hosts.size());
}

Block ReplicatedDatabaseQueryStatusSource::getSampleBlock()
{
    return Block{
        {std::make_shared<DataTypeString>(), "shard"},
        {std::make_shared<DataTypeString>(), "replica"},
        {getStatusEnum(), "status"},
        {std::make_shared<DataTypeUInt64>(), "num_hosts_remaining"},
        {std::make_shared<DataTypeUInt64>(), "num_hosts_active"},
    };
}
}
