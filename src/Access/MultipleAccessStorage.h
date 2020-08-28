#pragma once

#include <Access/IAccessStorage.h>
#include <Common/LRUCache.h>
#include <mutex>


namespace DB
{
/// Implementation of IAccessStorage which contains multiple nested storages.
class MultipleAccessStorage : public IAccessStorage
{
public:
    using Storage = IAccessStorage;

    MultipleAccessStorage(std::vector<std::unique_ptr<Storage>> nested_storages_);

    std::vector<UUID> findMultiple(EntityType type, const String & name) const;

    template <typename EntityType>
    std::vector<UUID> findMultiple(const String & name) const { return findMultiple(EntityType::TYPE, name); }

    const Storage * findStorage(const UUID & id) const;
    Storage * findStorage(const UUID & id);
    const Storage & getStorage(const UUID & id) const;
    Storage & getStorage(const UUID & id);

    void addStorage(std::unique_ptr<Storage> nested_storage);

    Storage & getStorageByIndex(size_t i) { return *(nested_storages[i]); }
    const Storage & getStorageByIndex(size_t i) const { return *(nested_storages[i]); }

protected:
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID &id) const override;
    bool canInsertImpl(const AccessEntityPtr & entity) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    ext::scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

private:
    std::vector<std::unique_ptr<Storage>> nested_storages;
    mutable LRUCache<UUID, Storage *> ids_cache;
    mutable std::mutex ids_cache_mutex;
};

}
