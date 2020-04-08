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

    MultipleAccessStorage(std::vector<std::unique_ptr<Storage>> nested_storages_, size_t index_of_nested_storage_for_insertion_ = 0);
    ~MultipleAccessStorage() override;

    std::vector<UUID> findMultiple(std::type_index type, const String & name) const;

    template <typename EntityType>
    std::vector<UUID> findMultiple(const String & name) const { return findMultiple(EntityType::TYPE, name); }

    const Storage * findStorage(const UUID & id) const;
    Storage * findStorage(const UUID & id);
    const Storage & getStorage(const UUID & id) const;
    Storage & getStorage(const UUID & id);

    Storage & getStorageByIndex(size_t i) { return *(nested_storages[i]); }
    const Storage & getStorageByIndex(size_t i) const { return *(nested_storages[i]); }

protected:
    std::optional<UUID> findImpl(std::type_index type, const String & name) const override;
    std::vector<UUID> findAllImpl(std::type_index type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID &id) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    SubscriptionPtr subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(std::type_index type) const override;

private:
    std::vector<std::unique_ptr<Storage>> nested_storages;
    IAccessStorage * nested_storage_for_insertion;
    mutable LRUCache<UUID, Storage *> ids_cache;
    mutable std::mutex ids_cache_mutex;
};

}
