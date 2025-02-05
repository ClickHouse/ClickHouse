#pragma once
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB 
{

class StorageZeroCopyReplicatedMergeTree: public StorageReplicatedMergeTree
{
public:
    // ctr


    class Transaction: public MergeTreeData::Transaction
    {
        explicit ZeroCopyTransaction(TransactionUniquePtr transaction)
        : parent_transaction(std::move(transaction))
        {
        }

        virtual DataPartsVector commit(DataPartsLock * acquired_parts_lock = nullptr)
        {           
            
            
        }

        virtual void addPart(MutableDataPartPtr & part, bool need_rename);

        virtual void rollback(DataPartsLock * lock = nullptr);
    
    private:
        TransactionUniquePtr parent_transaction;
    };

        /// Fetch part only when it stored on shared storage like S3
    MutableDataPartPtr executeFetchShared(const String & source_replica, const String & new_part_name, const DiskPtr & disk, const String & path);

    /// Lock part in zookeeper for use shared data in several nodes
    void lockSharedData(const IMergeTreeDataPart & part, bool replace_existing_lock, std::optional<HardlinkedFiles> hardlinked_files) const override;
    void lockSharedData(
        const IMergeTreeDataPart & part,
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        bool replace_existing_lock,
        std::optional<HardlinkedFiles> hardlinked_files) const;

    void getLockSharedDataOps(
        const IMergeTreeDataPart & part,
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        bool replace_existing_lock,
        std::optional<HardlinkedFiles> hardlinked_files,
        Coordination::Requests & requests) const;

    zkutil::EphemeralNodeHolderPtr lockSharedDataTemporary(const String & part_name, const String & part_id, const DiskPtr & disk) const;

    /// Unlock shared data part in zookeeper
    /// Return true if data unlocked
    /// Return false if data is still used by another node
    std::pair<bool, NameSet> unlockSharedData(const IMergeTreeDataPart & part) const override;
    std::pair<bool, NameSet>
    unlockSharedData(const IMergeTreeDataPart & part, const ZooKeeperWithFaultInjectionPtr & zookeeper) const;

    /// Unlock shared data part in zookeeper by part id
    /// Return true if data unlocked
    /// Return false if data is still used by another node
    static std::pair<bool, NameSet> unlockSharedDataByID(
        String part_id,
        const String & table_uuid,
        const MergeTreePartInfo & part_info,
        const String & replica_name_,
        const std::string & disk_type,
        const ZooKeeperWithFaultInjectionPtr & zookeeper_,
        const MergeTreeSettings & settings,
        LoggerPtr logger,
        const String & zookeeper_path_old,
        MergeTreeDataFormatVersion data_format_version);

    /// Fetch part only if some replica has it on shared storage like S3
    MutableDataPartPtr tryToFetchIfShared(const IMergeTreeDataPart & part, const DiskPtr & disk, const String & path) override;
    
    static bool removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
    const String & replica_name, const String & zookeeper_path, const ContextPtr & local_context, const zkutil::ZooKeeperPtr & zookeeper);

    bool canUseZeroCopyReplication() const;

    /// Create freeze metadata for table and save in zookeeper. Required only if zero-copy replication enabled.
    void createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr part, String backup_part_path) const override;

    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> fetchSelectedPart(
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const String & part_name,
        const String & zookeeper_name,
        const String & replica_path,
        const String & host,
        int port,
        const ConnectionTimeouts & timeouts,
        const String & user,
        const String & password,
        const String & interserver_scheme,
        ThrottlerPtr throttler,
        bool to_detached,
        const String & tmp_prefix_,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        DiskPtr disk) override;
    
    /// Create freeze metadata for table and save in zookeeper. Required only if zero-copy replication enabled.
    void createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr part, String backup_part_path) const override;

private:

    std::mutex existing_zero_copy_locks_mutex;

    struct ZeroCopyLockDescription
    {
        std::string replica;
        std::shared_ptr<std::atomic<bool>> exists;
    };

    std::unordered_map<String, ZeroCopyLockDescription> existing_zero_copy_locks;

    static Strings getZeroCopyPartPath(const MergeTreeSettings & settings, const std::string & disk_type, const String & table_uuid,
        const String & part_name, const String & zookeeper_path_old);

    static void createZeroCopyLockNode(
        const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node,
        int32_t mode = zkutil::CreateMode::Persistent, bool replace_existing_lock = false,
        const String & path_to_set_hardlinked_files = "", const NameSet & hardlinked_files = {});

    static void getZeroCopyLockNodeCreateOps(
        const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node, Coordination::Requests & requests,
        int32_t mode = zkutil::CreateMode::Persistent, bool replace_existing_lock = false,
        const String & path_to_set_hardlinked_files = "", const NameSet & hardlinked_files = {});

        bool checkZeroCopyLockExists(const String & part_name, const DiskPtr & disk, String & lock_replica);
    void watchZeroCopyLock(const String & part_name, const DiskPtr & disk);

    std::optional<String> getZeroCopyPartPath(const String & part_name, const DiskPtr & disk);

    /// Create ephemeral lock in zookeeper for part and disk which support zero copy replication.
    /// If no connection to zookeeper, shutdown, readonly -- return std::nullopt.
    /// If somebody already holding the lock -- return unlocked ZeroCopyLock object (not std::nullopt).
    std::optional<ZeroCopyLock> tryCreateZeroCopyExclusiveLock(const String & part_name, const DiskPtr & disk) override;

    /// Wait for ephemral lock to disappear. Return true if table shutdown/readonly/timeout exceeded, etc.
    /// Or if node actually disappeared.
    bool waitZeroCopyLockToDisappear(const ZeroCopyLock & lock, size_t milliseconds_to_wait) override;


    std::vector<String> getZookeeperZeroCopyLockPaths() const;
    static void dropZookeeperZeroCopyLockPaths(zkutil::ZooKeeperPtr zookeeper,
                                               std::vector<String> zero_copy_locks_paths, LoggerPtr logger);

    bool checkPartChecksumsAndAddCommitOps(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        const DataPartPtr & part,
        Coordination::Requests & ops,
        String part_name,
        NameSet & absent_replicas_paths) override;
    
    bool isPreferredFetchFromOtherReplicaForReplaceRange(PartDescriptionPtr & part_desc) override;
    bool isCloneNeededForReplaceRange(const DataPartPtr & part, const PartDescriptionPtr & part_desc) override;

    void createNewZooKeeperNodesAttempt() const override;

    void prepareDrop() override;
    void onLastReplicaDropped() override;

    ExecutableTaskPtr makeMutateFromLogEntryTask(const SelectedEntryPtr & selected_entry) const override;

    std::vector<String> zero_copy_lock_paths;

};

};

