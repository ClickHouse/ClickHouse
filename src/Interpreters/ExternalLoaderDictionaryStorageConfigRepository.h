#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Databases/IDatabase.h>

#include <memory>
#include <mutex>
#include <utility>

namespace DB
{

class ExternalDictionariesLoader;
class StorageDictionary;

class ExternalLoaderDictionaryStorageConfigRepository : public IExternalLoaderConfigRepository
{
public:
    class ActiveUUIDRepositoryLease final
    {
    public:
        ActiveUUIDRepositoryLease() = default;
        ActiveUUIDRepositoryLease(const ActiveUUIDRepositoryLease &) = delete;
        ActiveUUIDRepositoryLease & operator=(const ActiveUUIDRepositoryLease &) = delete;
        ActiveUUIDRepositoryLease(ActiveUUIDRepositoryLease && other) noexcept
            : lock(std::move(other.lock))
            , storage_owner(std::move(other.storage_owner))
            , storage(std::exchange(other.storage, nullptr))
        {
        }
        ActiveUUIDRepositoryLease & operator=(ActiveUUIDRepositoryLease && other) noexcept
        {
            if (this != &other)
            {
                release();
                lock = std::move(other.lock);
                storage_owner = std::move(other.storage_owner);
                storage = std::exchange(other.storage, nullptr);
            }
            return *this;
        }
        ~ActiveUUIDRepositoryLease() { release(); }

        explicit operator bool() const noexcept { return storage && lock.owns_lock(); }
        const StorageDictionary * getStorage() const noexcept { return lock.owns_lock() ? storage : nullptr; }

    private:
        friend class ExternalLoaderDictionaryStorageConfigRepository;

        ActiveUUIDRepositoryLease(
            std::unique_lock<std::mutex> lock_,
            std::shared_ptr<const StorageDictionary> storage_owner_,
            const StorageDictionary * storage_) noexcept
            : lock(std::move(lock_))
            , storage_owner(std::move(storage_owner_))
            , storage(storage_)
        {
        }

        void release() noexcept
        {
            if (lock.owns_lock())
                lock.unlock();
            storage_owner.reset();
            storage = nullptr;
        }

        std::unique_lock<std::mutex> lock;
        std::shared_ptr<const StorageDictionary> storage_owner;
        const StorageDictionary * storage = nullptr;
    };

    explicit ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_storage_);
    ~ExternalLoaderDictionaryStorageConfigRepository() override;

    /// Pins the registry entry of one exact UUID-backed StorageDictionary
    /// repository while its owner has not entered detach/drop/restart. The
    /// lease prevents repository teardown (and therefore destruction of its
    /// referenced storage) while a bootstrap decision is being completed.
    static ActiveUUIDRepositoryLease acquireActiveUUIDRepository(
        const ExternalDictionariesLoader & loader, const std::string & uuid, const StorageDictionary * expected_storage = nullptr);

    /// Replaces the constructor-only raw owner capability with a weak owner
    /// immediately after make_shared has established StorageDictionary's
    /// shared ownership. Every later lease must successfully pin that owner.
    [[nodiscard]] static bool bindActiveUUIDRepositoryStorage(
        const ExternalDictionariesLoader & loader, const std::string & uuid, const std::shared_ptr<StorageDictionary> & storage);

    std::string getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

private:
    const StorageDictionary & dictionary_storage;
    const ExternalDictionariesLoader * registered_loader = nullptr;
    std::string registered_uuid;
};

}
