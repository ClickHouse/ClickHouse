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
        const String & zk_node_path, const String & zk_replicas_path, ContextPtr context_, const Strings & hosts_to_wait);

    String getName() const override { return "ReplicatedDatabaseQueryStatus"; }

protected:
    ExecutionStatus checkStatus(const String & host_id) override;
    Chunk generateChunkWithUnfinishedHosts() const override;
    Strings getNodesToWait() override;
    Chunk handleTimeoutExceeded() override;
    Chunk stopWaitingOfflineHosts() override;
    void handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id) override;
    void fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns) override;

private:
    static Block getSampleBlock();

    enum ReplicatedDatabaseQueryStatus
    {
        /// Query is (successfully) finished
        OK = 0,
        /// Query is not finished yet, but replica is currently executing it
        IN_PROGRESS = 1,
        /// Replica is not available or busy with previous queries. It will process query asynchronously
        QUEUED = 2,
    };
};
}
