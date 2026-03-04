
#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/SyncReplicaMode.h>
#include <QueryPipeline/BlockIO.h>

#include <mutex>
#include <atomic>

namespace zkutil
{
class ZooKeeper;
}

namespace DB
{

class DDLReplicateWorker;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;
class ZooKeeperMetadataTransaction;
using ZooKeeperMetadataTransactionPtr = std::shared_ptr<ZooKeeperMetadataTransaction>;
class DDLGuard;
using DDLGuardPtr = std::unique_ptr<DDLGuard>;
struct DDLReplicatorSettings;

/// DDLReplicator holds the ZooKeeper coordination state shared by any entity
/// that replicates DDL operations through a sequential ZooKeeper log.
///
/// Each DDLReplicator is identified by:
///   - zookeeper_path  — root ZK path for the DDL log and metadata
///   - shard_name / replica_name — identity of this node within the cluster
///   - replica_path  — derived path <zookeeper_path>/replicas/<shard|replica>
///
/// A paired DDLReplicateWorker (DDLWorker subclass) reads entries from the log
/// and executes them on every replica that shares the same zookeeper_path.
///
/// Concrete subclasses include DatabaseReplicated (table-level DDL within a
/// single database) and ReplicatedDatabaseCatalog (database-level DDL across
/// the cluster).  Both follow the same replication protocol; only the scope
/// of the replicated operations differs.
class DDLReplicator
{
    friend class DDLReplicateWorker;
    friend struct DDLReplicateTask;
public:
    static constexpr auto REPLICA_UNSYNCED_MARKER = "\tUNSYNCED";

    virtual ~DDLReplicator() = default;
    virtual String getName() const = 0;

    const String & getZooKeeperName() const { return zookeeper_name; }
    const String & getZooKeeperPath() const { return zookeeper_path; }
    String getShardName() const { return shard_name; }
    String getReplicaName() const { return replica_name; }
    String getFullReplicaName() const;
    static String getFullReplicaName(const String & shard, const String & replica);
    static std::pair<String, String> parseFullReplicaName(const String & name);

    virtual const DDLReplicatorSettings & getSettings() const = 0;

    String getReplicaGroupName() const { return replica_group_name; }

    virtual bool waitForReplicaToProcessAllEntries(ContextPtr context_, UInt64 timeout_ms, SyncReplicaMode mode = SyncReplicaMode::DEFAULT); /// NOLINT
protected:
    DDLReplicator() = default;
    DDLReplicator(
        const String & zookeeper_name_,
        const String & zookeeper_path_,
        const String & shard_name_,
        const String & replica_name_);

    virtual LoggerPtr getLogger() const = 0;

    virtual String getHostID(ContextPtr global_context, bool secure) const;

    Coordination::Requests buildReplicatorNodesInZooKeeper();
    bool createDDLReplicatorNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);
    static bool looksLikeDDLReplicatorPath(const ZooKeeperPtr & current_zookeeper, const String & path, const String & mark);
    void createReplicaNodesInZooKeeper(ContextPtr context, const ZooKeeperPtr & current_zookeeper);

    virtual void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr) = 0;
    void createEmptyLogEntry(const ZooKeeperPtr & current_zookeeper);

    std::map<String, String> tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr) const;
    std::map<String, String> getConsistentMetadataSnapshotImpl(
        const ZooKeeperPtr & zookeeper,
        const std::function<bool(const String &)> & filter_by_name,
        size_t max_retries,
        UInt32 & max_log_ptr) const;

    virtual UInt64 getLocalDigest() const = 0;
    virtual bool checkDigestValid(const ContextPtr & local_context) const TSA_REQUIRES(metadata_mutex);
    void assertDigestWithProbability(const ContextPtr & local_context) const TSA_REQUIRES(metadata_mutex);
    /// Assert digest either inline or in transaction if it is internal query (using finalizer)
    /// (since in case of internal queries it is not submitted in place)
    void assertDigest(const ContextPtr & local_context) TSA_REQUIRES(metadata_mutex);
    /// If @txn is set check digest in transaction, otherwise - inline.
    void assertDigestInTransactionOrInline(const ContextPtr & local_context, const ZooKeeperMetadataTransactionPtr & txn) TSA_REQUIRES(metadata_mutex);

    static BlockIO getQueryStatus(
        const String & zookeeper_name,
        const String & node_path,
        const String & replicas_path,
        ContextPtr context,
        const Strings & hosts_to_wait,
        DDLGuardPtr && database_guard);

    ZooKeeperPtr getZooKeeper(ContextPtr context_) const;
    void shutdownDDLWorker();
    void resetDDLWorker();

    const String zookeeper_name;
    const String zookeeper_path;
    const String shard_name;
    const String replica_name;
    const String replica_path;

    /// Replica group is used to filter replicas that belong to the same logical group.
    /// Replicas within the same group form a cluster; cross-group queries are handled
    /// via the "all_groups" cluster.  Empty means all replicas are in one implicit group.
    /// Only used by DatabaseReplicated currently.
    String replica_group_name;

    std::atomic_bool is_readonly = true;
    std::atomic_bool is_recovering = false;
    std::atomic_bool ddl_worker_initialized = false;
    std::unique_ptr<DDLReplicateWorker> ddl_worker;
    mutable std::mutex ddl_worker_mutex;
    UInt32 max_log_ptr_at_creation = 0;

    /// Usually operation with metadata are single-threaded because of the way replication works,
    /// but StorageReplicatedMergeTree may call alterTable outside from DatabaseReplicatedDDLWorker causing race conditions.
    mutable std::mutex metadata_mutex;
    /// Sum of hashes of pairs (key, DDL).
    /// We calculate this sum from local metadata files and compare it will value in ZooKeeper.
    /// It allows to detect if metadata is broken and recover replica.
    UInt64 metadata_digest TSA_GUARDED_BY(metadata_mutex);
};

}
