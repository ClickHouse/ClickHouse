#pragma once

#include <ACL/IAttributesStorage.h>
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
    std::vector<UUID> findPrefixed(const String & prefix, const Type & type) const override;
    std::optional<UUID> find(const String & name, const Type & type) const override;
    bool exists(const UUID & id) const override;

protected:
    std::pair<UUID, bool> tryInsertImpl(const Attributes & attrs, AttributesPtr & caused_name_collision) override;
    bool tryRemoveImpl(const UUID & id) override;
    AttributesPtr tryReadImpl(const UUID & id) const override;
    void updateImpl(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func) override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const override;
    SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const override;

private:
    Storage * findStorageByID(const UUID & id) const;

    class SubscriptionForNew;

    std::vector<std::unique_ptr<Storage>> nested_storages;
    IAttributesStorage * nested_storage_for_insertion;

    using NameAndType = std::pair<String, const Type *>;
    using IDAndStorage = std::pair<std::optional<UUID>, Storage *>;

    struct NameAndTypeHash
    {
        size_t operator()(const NameAndType & pr) const;
    };

    mutable LRUCache<NameAndType, IDAndStorage, NameAndTypeHash> names_and_types_cache;
    mutable LRUCache<UUID, Storage *> ids_cache;
    mutable std::mutex mutex;
};

}
