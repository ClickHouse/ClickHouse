#pragma once

#include <Access/IAccessStorage.h>
#include <base/defines.h>
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

    explicit MemoryAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_, bool allow_backup_);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }
    void restoreFromBackup(RestorerFromBackup & restorer) override;

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool insertWithID(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists);
    bool insertNoLock(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) TSA_REQUIRES(mutex);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists) TSA_REQUIRES(mutex);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) TSA_REQUIRES(mutex);

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
    };

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id TSA_GUARDED_BY(mutex); /// We want to search entries both by ID and by the pair of name and type.
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)] TSA_GUARDED_BY(mutex);
    AccessChangesNotifier & changes_notifier;
    bool backup_allowed = false;
};
}
