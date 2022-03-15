#pragma once

#include <Access/IAccessStorage.h>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{
/// Implementation of IAccessStorage which keeps all data in memory.
class MemoryAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "memory";

    explicit MemoryAccessStorage(const String & storage_name_ = STORAGE_TYPE);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

    bool exists(const UUID & id) const override;
    bool hasSubscription(const UUID & id) const override;
    bool hasSubscription(AccessEntityType type) const override;

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const override;

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
        mutable std::list<OnChangedHandler> handlers_by_id;
    };

    bool insertNoLock(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, Notifications & notifications);
    bool removeNoLock(const UUID & id, bool throw_if_not_exists, Notifications & notifications);
    bool updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, Notifications & notifications);
    void setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications);
    void prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const;

    mutable std::recursive_mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id; /// We want to search entries both by ID and by the pair of name and type.
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(AccessEntityType::MAX)];
};
}
