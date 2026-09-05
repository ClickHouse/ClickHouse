#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DistributedQueryStatusSource.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>

namespace DB
{
class ReplicatedDatabaseQueryStatusSource final : public DistributedQueryStatusSource
{
public:
    ReplicatedDatabaseQueryStatusSource(
        const String & zookeeper_name_,
        const String & zk_node_path,
        const String & zk_replicas_path,
        ContextPtr context_,
        const Strings & hosts_to_wait,
        DDLGuardPtr && database_guard_);

    String getName() const override { return "ReplicatedDatabaseQueryStatus"; }

protected:
    ExecutionStatus checkStatus(const String & host_id) override;
    /// Only opt into the atomic list-with-data when we wait on finished/<host_id> (whose payload is a serialized
    /// ExecutionStatus). Under database_replicated_enforce_synchronous_settings getNodesToWait() waits on synced/,
    /// whose nodes carry an empty payload; caching that would make checkStatus deserialize "" and keep the (-1)
    /// sentinel, so fall back to the per-host finished/ get in that mode.
    bool wantsFinishedNodeData() const override;
    Chunk generateChunkWithUnfinishedHosts() const override;
    Strings getNodesToWait() override;
    Chunk handleTimeoutExceeded() override;
    Chunk stopWaitingOfflineHosts() override;
    void handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id) override;
    void fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns) override;

private:
    static Block getSampleBlock();
    /// A kind of read lock for the database which prevents dropping the database (and its metadata from zk that we use for getting the query status)
    DDLGuardPtr database_guard;


};
}
