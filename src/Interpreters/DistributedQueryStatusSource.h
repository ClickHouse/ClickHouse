#pragma once

#include <filesystem>
#include <map>
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

    /// When true (and the connected Keeper advertises LIST_WITH_STAT_AND_DATA), generate() lists the finished
    /// nodes together with their data in a single atomic request and caches it, so a subclass can read a finished
    /// node's status without a separate get that could race the cleaner. Defaults to false: the ON CLUSTER path
    /// keeps the plain listing.
    virtual bool wantsFinishedNodeData() const { return false; }

    Strings getNewAndUpdate(const Strings & current_finished_hosts);
    /// When node_exists is provided it reports whether the status node was present. An absent node yields the same
    /// (-1, "Cannot obtain error message") sentinel as a present-but-unreadable one, so callers that must tell the
    /// two apart (see ReplicatedDatabaseQueryStatusSource::checkStatus) pass node_exists.
    ExecutionStatus getExecutionStatus(const fs::path & status_path, bool * node_exists = nullptr);

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
    /// finished/<host_id> -> payload, populated in generate() only when wantsFinishedNodeData() and the Keeper
    /// advertises LIST_WITH_STAT_AND_DATA. Lets a subclass read a just-listed finished node's status atomically.
    std::map<String, String> finished_node_data;
    /// True for the current generate() pass when finished_node_data was filled from an atomic list-with-data.
    bool finished_node_data_available = false;
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
