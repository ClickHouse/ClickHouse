#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

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

    ReplicatedAccessStorage(const String & storage_name, const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper, AccessChangesNotifier & changes_notifier_);
    virtual ~ReplicatedAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }

    void startPeriodicReloading() override { startWatchingThread(); }
    void stopPeriodicReloading() override { stopWatchingThread(); }

    bool exists(const UUID & id) const override;

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
    void refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);

    void setEntityNoLock(const UUID & id, const AccessEntityPtr & entity);
    void removeEntityNoLock(const UUID & id);

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
    };

    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];
    AccessChangesNotifier & changes_notifier;
};
}
