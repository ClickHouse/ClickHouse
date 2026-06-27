#pragma once

#include <Common/Clusters/ClusterMetadataMutation.h>
#include <Common/Clusters/ClusterMetadataStorage.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/DDLWorker.h>

#include <functional>
#include <memory>
#include <mutex>

namespace DB
{

/// Replicates SQL-managed cluster metadata mutations for one replica group.
///
/// It owns the Keeper participant state and applies committed metadata mutations
/// from the replica group's ordered DDL log.
class ClusterMetadataDDLWorker : public DDLWorker
{
public:
    using SnapshotReloader = std::function<String()>;
    using MutationApplier = std::function<String(const ClusterMetadataMutation &)>;

    ClusterMetadataDDLWorker(
        ContextPtr context_,
        ClusterMetadataStoragePtr storage_,
        String node_name_,
        String zookeeper_name_,
        UInt32 max_log_entries_per_batch_,
        SnapshotReloader snapshot_reloader_,
        MutationApplier mutation_applier_);
    ~ClusterMetadataDDLWorker() override;

    void shutdown() override;
    UInt32 getLogPointer() const;
    UInt32 getMaxLogPointer() const;
    const String & getNodeName() const { return node_name; }

    /// Process currently committed entries synchronously. Useful for startup and tests.
    bool processCommittedEntriesOnce();
    String enqueueMutation(const ClusterMetadataMutation & mutation);
    void enqueueMutationAndWait(const ClusterMetadataMutation & mutation);

private:
    ClusterMetadataStoragePtr storage;
    String node_name;
    SnapshotReloader snapshot_reloader;
    MutationApplier mutation_applier;
    UInt32 max_log_entries_per_batch = 1;

    String replica_group_root;
    String log_root;
    String counter_lock_path;
    String max_log_ptr_path;
    String logs_to_keep_path;
    String replicas_root;
    String replica_path;
    String replica_log_ptr_path;
    String replica_digest_path;
    String replica_active_path;

    zkutil::ZooKeeperPtr active_node_holder_zookeeper;
    zkutil::EphemeralNodeHolderPtr active_node_holder;
    mutable std::mutex processing_mutex;

    void scheduleTasks(bool reinitialized) override;
    void initializeReplication() override;
    void createReplicaDirs(const ZooKeeperPtr &, const NameSet &) override {}
    void markReplicasActive(bool reinitialized) override;
    DDLTaskPtr initAndCheckTask(const String &, String &, const ZooKeeperPtr &, bool) override { return {}; }
    bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat) override;

    void initKeeperLayout();
    void initializeCounter();
    void registerReplica();
    bool reloadSnapshotAndAdvanceIfTooFarBehind(UInt32 log_ptr, UInt32 max_log_ptr, UInt32 logs_to_keep);
    void appendMutationOps(Coordination::Requests & ops, const ClusterMetadataMutation & mutation) const;
    UInt32 processEntriesBatch(UInt32 first_entry, UInt32 last_entry);
    void updateReplicaDigest(const String & digest);
    bool canRemoveEntry(UInt32 entry_number) const;

    UInt32 readUInt32Node(const ZooKeeperPtr & zookeeper, const String & path) const;
    void setLogPointer(UInt32 log_pointer);
    UInt32 logEntryNumber(const String & entry_name) const;
};

using ClusterMetadataDDLWorkerPtr = std::shared_ptr<ClusterMetadataDDLWorker>;

}
