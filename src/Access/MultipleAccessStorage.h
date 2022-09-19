#pragma once

#include <Access/IAccessStorage.h>
#include <base/defines.h>
#include <Common/CacheBase.h>
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

    explicit MultipleAccessStorage(const String & storage_name_ = STORAGE_TYPE);
    ~MultipleAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    bool isReadOnly() const override;
    bool isReadOnly(const UUID & id) const override;

    void reload() override;
    void startPeriodicReloading() override;
    void stopPeriodicReloading() override;

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

    bool exists(const UUID & id) const override;

    bool isBackupAllowed() const override;
    bool isRestoreAllowed() const override;
    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, AccessEntityType type) const override;
    void restoreFromBackup(RestorerFromBackup & restorer) override;

protected:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;
    std::optional<UUID> authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const override;

private:
    using Storages = std::vector<StoragePtr>;
    std::shared_ptr<const Storages> getStoragesInternal() const;

    std::shared_ptr<const Storages> nested_storages TSA_GUARDED_BY(mutex);
    mutable CacheBase<UUID, Storage> ids_cache TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}
