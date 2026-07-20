#pragma once

#include <Access/IAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ZooKeeperReplicator.h>

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
    String getReplicationID() const override { return replicator.getReplicationID(); }

    void startPeriodicReloading() override { replicator.startWatchingThread(); }
    void stopPeriodicReloading() override { replicator.stopWatchingThread(); }
    void reload(ReloadMode reload_mode) override;

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }

private:
    bool insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;

    MemoryAccessStorage memory_storage;
    ZooKeeperReplicator replicator;
    const bool backup_allowed = false;
};
}
