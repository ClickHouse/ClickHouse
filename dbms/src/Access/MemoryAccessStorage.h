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
    MemoryAccessStorage(const String & storage_name_ = "memory");
    ~MemoryAccessStorage() override;

    /// Sets all entities at once.
    void setAll(const std::vector<AccessEntityPtr> & all_entities);
    void setAll(const std::vector<std::pair<UUID, AccessEntityPtr>> & all_entities);

private:
    std::optional<UUID> findImpl(std::type_index type, const String & name) const override;
    std::vector<UUID> findAllImpl(std::type_index type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    SubscriptionPtr subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(std::type_index type) const override;

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

    using NameTypePair = std::pair<String, std::type_index>;
    struct Hash
    {
        size_t operator()(const NameTypePair & key) const
        {
            return std::hash<String>{}(key.first) - std::hash<std::type_index>{}(key.second);
        }
    };

    mutable std::mutex mutex;
    std::unordered_map<UUID, Entry> entries;               /// We want to search entries both by ID and by the pair of name and type.
    std::unordered_map<NameTypePair, Entry *, Hash> names; /// and by the pair of name and type.
    mutable std::unordered_multimap<std::type_index, OnChangedHandler> handlers_by_type;
    std::shared_ptr<const MemoryAccessStorage *> shared_ptr_to_this; /// We need weak pointers to `this` to implement subscriptions.
};
}
