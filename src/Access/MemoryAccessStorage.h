#pragma once

#include <Access/IAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <Common/CurrentMetrics.h>
#include <base/defines.h>
#include <mutex>
#include <unordered_map>
#include <boost/container/flat_set.hpp>


namespace CurrentMetrics
{
    extern const Metric AttachedAccessEntity;
}

namespace DB
{

/// Implementation of IAccessStorage which keeps all data in memory.
class MemoryAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "memory";

    explicit MemoryAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_, bool allow_backup_, UInt64 access_entities_num_limit_);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    /// Removes all entities except the specified list `ids_to_keep`.
    /// The function skips IDs not contained in the storage.
    void removeAllExcept(const std::vector<UUID> & ids_to_keep);

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override { return backup_allowed; }

protected:
    struct Entry
    {
        UUID id;
        String name;
        AccessEntityType type;
        mutable AccessEntityPtr entity; /// may be nullptr, if the entity hasn't been loaded yet.
    };

    /// Insert or remove entires in maps, notify changes_notifier, count attached entities.
    void insertEntry(UUID id, String name, AccessEntityType type, AccessEntityPtr entity);
    void removeEntry(UUID id, String name, AccessEntityType type);

    bool entityLimitReached(UInt64 entity_count) const
    {
        return access_entities_num_limit > 0 && entity_count >= access_entities_num_limit;
    }

    AccessChangesNotifier & changes_notifier;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];

    mutable std::recursive_mutex mutex; // Note: Reentrace possible via LDAPAccessStorage

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    bool insertImpl(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    bool insertNoLock(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists);

    void removeAllExceptNoLock(const std::vector<UUID> & ids_to_keep);
    void removeAllExceptNoLock(const boost::container::flat_set<UUID> & ids_to_keep);

    const bool backup_allowed = false;
};
}
