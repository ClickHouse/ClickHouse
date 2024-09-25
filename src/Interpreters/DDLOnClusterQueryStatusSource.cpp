#include <unordered_set>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/DDLOnClusterQueryStatusSource.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>

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

DDLOnClusterQueryStatusSource::DDLOnClusterQueryStatusSource(
    const String & zk_node_path, const String & zk_replicas_path, ContextPtr context_, const Strings & hosts_to_wait)
    : DistributedQueryStatusSource(
          zk_node_path, zk_replicas_path, getSampleBlock(context_), context_, hosts_to_wait, "DDLOnClusterQueryStatusSource")
{
}

ExecutionStatus DDLOnClusterQueryStatusSource::checkStatus(const String & host_id)
{
    fs::path status_path = fs::path(node_path) / "finished" / host_id;
    return getExecutionStatus(status_path);
}

Chunk DDLOnClusterQueryStatusSource::generateChunkWithUnfinishedHosts() const
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
        auto [host, port] = parseHostAndPort(host_id);
        columns[num++]->insert(host);
        columns[num++]->insert(port);
        columns[num++]->insert(Field{});
        columns[num++]->insert(Field{});
        columns[num++]->insert(unfinished_hosts.size());
        columns[num++]->insert(current_active_hosts.size());
    }
    return Chunk(std::move(columns), unfinished_hosts.size());
}

Strings DDLOnClusterQueryStatusSource::getNodesToWait()
{
    return {String(fs::path(node_path) / "finished"), String(fs::path(node_path) / "active")};
}
Chunk DDLOnClusterQueryStatusSource::handleTimeoutExceeded()
{
    timeout_exceeded = true;

    size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
    size_t num_active_hosts = current_active_hosts.size();

    constexpr auto msg_format = "Distributed DDL task {} is not finished on {} of {} hosts "
                                "({} of them are currently executing the task, {} are inactive). "
                                "They are going to execute the query in background. Was waiting for {} seconds{}";

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

        return {};
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
Chunk DDLOnClusterQueryStatusSource::stopWaitingOfflineHosts()
{
    // Same logic as timeout exceeded
    return handleTimeoutExceeded();
}
void DDLOnClusterQueryStatusSource::handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id)
{
    assert(status.code != 0);

    if (!first_exception && context->getSettingsRef()[Setting::distributed_ddl_output_mode] != DistributedDDLOutputMode::NEVER_THROW)
    {
        auto [host, port] = parseHostAndPort(host_id);
        first_exception
            = std::make_unique<Exception>(Exception(status.code, "There was an error on [{}:{}]: {}", host, port, status.message));
    }
}
void DDLOnClusterQueryStatusSource::fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns)
{
    size_t num = 0;
    auto [host, port] = parseHostAndPort(host_id);
    columns[num++]->insert(host);
    columns[num++]->insert(port);
    columns[num++]->insert(status.code);
    columns[num++]->insert(status.message);
    columns[num++]->insert(waiting_hosts.size() - num_hosts_finished);
    columns[num++]->insert(current_active_hosts.size());
}

Block DDLOnClusterQueryStatusSource::getSampleBlock(ContextPtr context_)
{
    auto output_mode = context_->getSettingsRef()[Setting::distributed_ddl_output_mode];

    auto maybe_make_nullable = [&](const DataTypePtr & type) -> DataTypePtr
    {
        if (output_mode == DistributedDDLOutputMode::THROW || output_mode == DistributedDDLOutputMode::NONE
            || output_mode == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
            return type;
        return std::make_shared<DataTypeNullable>(type);
    };


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
