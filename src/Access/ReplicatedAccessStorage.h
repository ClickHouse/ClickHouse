#pragma once

#include <atomic>

#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <Access/MemoryAccessStorage.h>


namespace DB
{
class AccessChangesNotifier;

/// Implementation of IAccessStorage which keeps all data in zookeeper.
class ReplicatedAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "replicated";

    ReplicatedAccessStorage(const String & storage_name, const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper, AccessChangesNotifier & changes_notifier_, bool allow_backup);
    ~ReplicatedAccessStorage() override;

    void shutdown() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    bool isReplicated() const override { return true; }

    void startPeriodicReloading() override { startWatchingThread(); }
    void stopPeriodicReloading() override { stopWatchingThread(); }
    void reload(ReloadMode reload_mode) override;

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }
    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, AccessEntityType type) const override;

private:
    String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;

    zkutil::ZooKeeperPtr cached_zookeeper TSA_GUARDED_BY(cached_zookeeper_mutex);
    std::mutex cached_zookeeper_mutex;

    std::atomic<bool> watching = false;
    std::unique_ptr<ThreadFromGlobalPool> watching_thread;
    std::shared_ptr<ConcurrentBoundedQueue<UUID>> watched_queue;

    bool insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool insertZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id);
    bool removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, bool throw_if_not_exists);
    bool updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists);

    void initZooKeeperWithRetries(size_t max_retries);
    void initZooKeeperIfNeeded();
    zkutil::ZooKeeperPtr getZooKeeper();
    zkutil::ZooKeeperPtr getZooKeeperNoLock() TSA_REQUIRES(cached_zookeeper_mutex);
    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    void startWatchingThread();
    void stopWatchingThread();

    void runWatchingThread();
    void resetAfterError();

    bool refresh();
    void refreshEntities(const zkutil::ZooKeeperPtr & zookeeper, bool all);
    void refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);
    void refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) TSA_REQUIRES(mutex);

    AccessEntityPtr tryReadEntityFromZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) const;
    void setEntityNoLock(const UUID & id, const AccessEntityPtr & entity) TSA_REQUIRES(mutex);
    void removeEntityNoLock(const UUID & id) TSA_REQUIRES(mutex);

    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    bool acquireReplicatedRestore(RestorerFromBackup & restorer) const override;

    mutable std::mutex mutex;
    MemoryAccessStorage memory_storage TSA_GUARDED_BY(mutex);
    AccessChangesNotifier & changes_notifier;
    const bool backup_allowed = false;
};
}
