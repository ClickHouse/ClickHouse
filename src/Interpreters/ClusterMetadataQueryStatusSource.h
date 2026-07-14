#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedQueryStatusSource.h>

#include <unordered_map>

namespace DB
{

/// Status source for SQL catalog metadata DDL with `SYNC`.
///
/// Waits for registered replicas in the local `replica_group` to write
/// `log/query-N/finished/<uuid>` after applying the committed metadata mutation.
/// Display uses each replica's published `host:port` (stored in `replicas/<uuid>` node data).
class ClusterMetadataQueryStatusSource final : public DistributedQueryStatusSource
{
public:
    ClusterMetadataQueryStatusSource(
        const String & zookeeper_name_,
        const String & zk_node_path,
        const String & zk_replicas_path,
        ContextPtr context_,
        const Strings & hosts_to_wait);

    String getName() const override { return "ClusterMetadataQueryStatus"; }

protected:
    ExecutionStatus checkStatus(const String & host_id) override;
    Chunk generateChunkWithUnfinishedHosts() const override;
    Strings getNodesToWait() override;
    Chunk handleTimeoutExceeded() override;
    Chunk stopWaitingOfflineHosts() override;
    void handleNonZeroStatusCode(const ExecutionStatus & status, const String & host_id) override;
    void fillHostStatus(const String & host_id, const ExecutionStatus & status, MutableColumns & columns) override;

private:
    DistributedDDLOutputMode output_mode;
    /// Replica leaf name (`ServerUUID`) -> published display address (`host:port`).
    std::unordered_map<String, String> replica_addresses;

    void loadReplicaAddresses();
    String formatReplicaLabel(const String & replica_id) const;
    static std::pair<String, UInt16> parseDisplayAddress(const String & label);

    static bool nullableErrorAndStatusFields(DistributedDDLOutputMode output_mode);
    static Block getSampleBlock(DistributedDDLOutputMode output_mode);
};

}
