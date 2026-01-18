#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <mutex>

#include <base/defines.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Access/IAccessStorage.h>
#include <Access/MemoryAccessStorage.h>

namespace DB
{
class AccessChangesNotifier;

/// Class that handles replication of access entities using ZooKeeper.
/// This can be used by different access storage implementations to provide replication.
class ZooKeeperReplicator
{
public:
    ZooKeeperReplicator(
        const String & storage_name_,
        const String & zookeeper_path_,
        zkutil::GetZooKeeper get_zookeeper_,
        AccessChangesNotifier & changes_notifier_,
        MemoryAccessStorage & memory_storage_);

    ~ZooKeeperReplicator();

    void shutdown();

    /// Start and stop the thread watching for ZooKeeper changes
    void startWatchingThread();
    void stopWatchingThread();
    void runWatchingThread();
    void resetAfterError();

    /// Reload entities from ZooKeeper
    void reload(bool force_reload_all);

    /// ZooKeeper operations for entities
    bool insertEntity(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id);
    bool removeEntity(const UUID & id, bool throw_if_not_exists);
    bool updateEntity(const UUID & id, const IAccessStorage::UpdateFunc & update_func, bool throw_if_not_exists);
    std::optional<UUID> findEntity(AccessEntityType type, const String & name) const;
    std::vector<UUID> findAllEntities(AccessEntityType type) const;
    AccessEntityPtr readEntity(const UUID & id, bool throw_if_not_exists) const;

    /// Check if entity exists in ZooKeeper
    bool exists(const UUID & id) const;

    /// Get ZooKeeper replication ID
    String getReplicationID() const { return zookeeper_path; }

    /// Get current ZooKeeper connection
    zkutil::ZooKeeperPtr getZooKeeper();

private:
    String storage_name;
    String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;

    zkutil::ZooKeeperPtr cached_zookeeper TSA_GUARDED_BY(cached_zookeeper_mutex);
    std::mutex cached_zookeeper_mutex;

    std::atomic<bool> watching = false;
    std::unique_ptr<ThreadFromGlobalPool> watching_thread;
    std::shared_ptr<ConcurrentBoundedQueue<UUID>> watched_queue;

    Coordination::WatchCallbackPtr watch_entities_list;

    MemoryAccessStorage & memory_storage TSA_GUARDED_BY(mutex);
    AccessChangesNotifier & changes_notifier;

    bool insertZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id);
    bool removeZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, bool throw_if_not_exists);
    bool updateZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id, const IAccessStorage::UpdateFunc & update_func, bool throw_if_not_exists);

    void initZooKeeperWithRetries(size_t max_retries);
    void initZooKeeperIfNeeded();
    zkutil::ZooKeeperPtr getZooKeeperNoLock() TSA_REQUIRES(cached_zookeeper_mutex);
    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    bool refresh();
    void refreshEntities(const zkutil::ZooKeeperPtr & zookeeper, bool all);
    void refreshEntity(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id);
    void refreshEntityNoLock(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) TSA_REQUIRES(mutex);

    AccessEntityPtr tryReadEntityFromZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const UUID & id) const;
    void setEntityNoLock(const UUID & id, const AccessEntityPtr & entity)  TSA_REQUIRES(mutex);
    void removeEntityNoLock(const UUID & id) TSA_REQUIRES(mutex);

    mutable std::mutex mutex;
};

}
