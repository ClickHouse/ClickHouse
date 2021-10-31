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
/// Implementation of IAccessStorage which keeps all data in zookeeper.
class ReplicatedAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "replicated";

    ReplicatedAccessStorage(const String & storage_name, const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper);
    virtual ~ReplicatedAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }

    virtual void startup();
    virtual void shutdown();

private:
    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;

    std::atomic<bool> initialized = false;
    std::atomic<bool> stop_flag = false;
    ThreadFromGlobalPool worker_thread;
    ConcurrentBoundedQueue<UUID> refresh_queue;

    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;

    void insertZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists);
    void removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);
    void updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const UpdateFunc & update_func);

    void runWorkerThread();
    void resetAfterError();
    void initializeZookeeper();
    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    void refresh();
    void refreshEntities(const zkutil::ZooKeeperPtr & zookeeper);
    void refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);
    void refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, Notifications & notifications);

    void setEntityNoLock(const UUID & id, const AccessEntityPtr & entity, Notifications & notifications);
    void removeEntityNoLock(const UUID & id, Notifications & notifications);

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
        mutable std::list<OnChangedHandler> handlers_by_id;
    };

    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr &) const override { return true; }

    void prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)];
};
}
