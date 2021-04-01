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

    MemoryAccessStorage(const String & storage_name_ = STORAGE_TYPE);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

private:
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr &) const override { return true; }
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    ext::scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

    struct Entry
    {
        UUID id;
        AccessEntityPtr entity;
        mutable std::list<OnChangedHandler> handlers_by_id;
    };

    void insertNoLock(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, Notifications & notifications);
    void removeNoLock(const UUID & id, Notifications & notifications);
    void updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications);
    void setAllNoLock(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities, Notifications & notifications);
    void prepareNotifications(const Entry & entry, bool remove, Notifications & notifications) const;

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id; /// We want to search entries both by ID and by the pair of name and type.
    std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)];
};
}
