#pragma once

#include <filesystem>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DDLTask.h>
#include <Processors/ISource.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>

namespace fs = std::filesystem;

namespace DB
{
class DistributedQueryStatusSource : public ISource
{
public:
    DistributedQueryStatusSource(
        const String & zookeeper_name_,
        const String & zk_node_path,
        const String & zk_replicas_path,
        SharedHeader block,
        ContextPtr context_,
        const Strings & hosts_to_wait,
        const char * logger_name);

    Chunk generate() override;
    Status prepare() override;

protected:
    virtual ExecutionStatus checkStatus(const String & host_id) = 0;
    virtual Chunk generateChunkWithUnfinishedHosts() const = 0;
    virtual Strings getNodesToWait() = 0;
    virtual Chunk handleTimeoutExceeded() = 0;
    virtual Chunk stopWaitingOfflineHosts() = 0;
    virtual void handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id) = 0;
    virtual void fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns) = 0;

    virtual NameSet getOfflineHosts(const NameSet & hosts_to_wait, const ZooKeeperPtr & zookeeper);

    Strings getNewAndUpdate(const Strings & current_finished_hosts);
    ExecutionStatus getExecutionStatus(const fs::path & status_path);

    ZooKeeperRetriesInfo getRetriesInfo() const;
    static std::pair<String, UInt16> parseHostAndPort(const String & host_id);
    static std::shared_ptr<DataTypeEnum8> getStatusEnum();

    enum class QueryStatus
    {
        /// Query is (successfully) finished
        OK = 0,
        /// Query is not finished yet, but replica is currently executing it
        IN_PROGRESS = 1,
        /// Replica is not available or busy with previous queries. It will process query asynchronously
        QUEUED = 2,
        /// Query is timed out or the replica is offline
        UNFINISHED = 3,
    };

    String zookeeper_name;
    String node_path;
    String replicas_path;
    ContextPtr context;
    Stopwatch watch;
    LoggerPtr log;

    NameSet waiting_hosts; /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that are currently executing the task
    NameSet offline_hosts; /// Hosts that are not currently running
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
    bool throw_on_timeout = true;
    bool throw_on_timeout_only_active = false;
    bool only_running_hosts = false;

    bool timeout_exceeded = false;
    bool stop_waiting_offline_hosts = false;
};
}
