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
    static constexpr char STORAGE_TYPE[] = "multiple";

    using Storage = IAccessStorage;
    using StoragePtr = std::shared_ptr<Storage>;
    using ConstStoragePtr = std::shared_ptr<const Storage>;

    MultipleAccessStorage(const String & storage_name_ = STORAGE_TYPE);
    ~MultipleAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }

    void setStorages(const std::vector<StoragePtr> & storages);
    void addStorage(const StoragePtr & new_storage);
    void removeStorage(const StoragePtr & storage_to_remove);
    std::vector<StoragePtr> getStorages();
    std::vector<ConstStoragePtr> getStorages() const;
    std::shared_ptr<const std::vector<StoragePtr>> getStoragesPtr();

    ConstStoragePtr findStorage(const UUID & id) const;
    StoragePtr findStorage(const UUID & id);
    ConstStoragePtr getStorage(const UUID & id) const;
    StoragePtr getStorage(const UUID & id);

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
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;
    UUID loginImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators) const override;
    UUID getIDOfLoggedUserImpl(const String & user_name) const override;

private:
    using Storages = std::vector<StoragePtr>;
    std::shared_ptr<const Storages> getStoragesInternal() const;
    void updateSubscriptionsToNestedStorages(std::unique_lock<std::mutex> & lock) const;

    std::shared_ptr<const Storages> nested_storages;
    mutable LRUCache<UUID, Storage> ids_cache;
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::unordered_map<StoragePtr, scope_guard> subscriptions_to_nested_storages[static_cast<size_t>(EntityType::MAX)];
    mutable std::mutex mutex;
};

}
