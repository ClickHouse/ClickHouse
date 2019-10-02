#pragma once

#include <AccessControl/IAttributesStorage.h>
#include <Common/LRUCache.h>
#include <mutex>


namespace DB
{
/// Implementation of IAttributesStorage which contains multiple nested storages.
class MultipleAttributesStorage : public IAttributesStorage
{
public:
    using Storage = IAttributesStorage;

    MultipleAttributesStorage(std::vector<std::unique_ptr<Storage>> nested_storages_, size_t index_of_nested_storage_for_insertion_ = 0);
    ~MultipleAttributesStorage() override;
    const String & getStorageName() const override;

protected:
    std::vector<UUID> findPrefixedImpl(const String & prefix, const Type & type) const override;
    std::optional<UUID> findImpl(const String & name, const Type & type) const override;
    bool existsImpl(const UUID & id) const override;
    AttributesPtr readImpl(const UUID & id) const override;
    std::pair<String, const Type *> readNameAndTypeImpl(const UUID &id) const override;
    UUID insertImpl(const IAttributes & attrs, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const override;

private:
    Storage * findStorageByID(const UUID & id) const;
    Storage & getStorageByID(const UUID & id) const;

    class SubscriptionForNew;

    std::vector<std::unique_ptr<Storage>> nested_storages;
    IAttributesStorage * nested_storage_for_insertion;

    using NameAndType = std::pair<String, const Type *>;
    using IDAndStorage = std::pair<UUID, Storage *>;

    struct NameAndTypeHash
    {
        size_t operator()(const NameAndType & pr) const;
    };

    mutable LRUCache<NameAndType, IDAndStorage, NameAndTypeHash> names_and_types_cache;
    mutable LRUCache<UUID, Storage *> ids_cache;
    mutable std::mutex mutex;
};

}
