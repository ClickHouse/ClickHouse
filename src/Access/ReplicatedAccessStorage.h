#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <Access/IAccessStorage.h>


namespace DB
{
class AccessChangesNotifier;

/// Implementation of IAccessStorage which keeps all data in zookeeper.
class ReplicatedAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "replicated";

    ReplicatedAccessStorage(const String & storage_name, const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper, AccessChangesNotifier & changes_notifier_, bool allow_backup);
    virtual ~ReplicatedAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }

    void startPeriodicReloading() override { startWatchingThread(); }
    void stopPeriodicReloading() override { stopWatchingThread(); }

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }
    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, AccessEntityType type) const override;
    void restoreFromBackup(RestorerFromBackup & restorer) override;

private:
    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;

    std::atomic<bool> initialized = false;

    std::atomic<bool> watching = false;
    ThreadFromGlobalPool watching_thread;
    std::shared_ptr<ConcurrentBoundedQueue<UUID>> watched_queue;

    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool insertWithID(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists);
    bool insertZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists);
    bool removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, bool throw_if_not_exists);
    bool updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists);

    void initializeZookeeper();
    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    void startWatchingThread();
    void stopWatchingThread();

    void runWatchingThread();
    void resetAfterError();

    bool refresh();
    void refreshEntities(const zkutil::ZooKeeperPtr & zookeeper);
    void refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);
    void refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) TSA_REQUIRES(mutex);

    void setEntityNoLock(const UUID & id, const AccessEntityPtr & entity) TSA_REQUIRES(mutex);
    void removeEntityNoLock(const UUID & id) TSA_REQUIRES(mutex);

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
    };

    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id TSA_GUARDED_BY(mutex);
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)] TSA_GUARDED_BY(mutex);
    AccessChangesNotifier & changes_notifier;
    bool backup_allowed = false;
};
}
