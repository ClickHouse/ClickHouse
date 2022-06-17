#pragma once

#include <Access/IAccessStorage.h>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{
class AccessChangesNotifier;

/// Implementation of IAccessStorage which keeps all data in memory.
class MemoryAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "memory";

    explicit MemoryAccessStorage(const String & storage_name_, bool allow_backup_, AccessChangesNotifier & changes_notifier_);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }
    std::vector<std::pair<UUID, AccessEntityPtr>> readAllForBackup(AccessEntityType type, const BackupSettings & backup_settings) const override;
    void insertFromBackup(const std::vector<std::pair<UUID, AccessEntityPtr>> & entities_from_backup, const RestoreSettings & restore_settings, std::shared_ptr<IRestoreCoordination> restore_coordination) override;

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool insertWithID(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists);
    bool insertNoLock(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists);

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
    };

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id; /// We want to search entries both by ID and by the pair of name and type.
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];
    bool backup_allowed = false;
    AccessChangesNotifier & changes_notifier;
};
}
