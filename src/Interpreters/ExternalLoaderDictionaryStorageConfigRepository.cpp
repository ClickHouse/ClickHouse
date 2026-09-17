#include <Interpreters/ExternalLoaderDictionaryStorageConfigRepository.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Storages/StorageDictionary.h>

#include <atomic>
#include <mutex>
#include <string_view>
#include <unordered_map>

namespace DB
{
namespace FailPoints
{
extern const char udt_dictionary_repository_pause_after_live_admission[];
}

namespace
{

constexpr std::string_view dictionary_repository_failpoint_query_id_prefix = "udt_dictionary_repository_load_";

struct ActiveDictionaryStorageRepositories
{
    std::mutex mutex;
    struct StorageRegistration
    {
        size_t repository_count = 0;
        std::weak_ptr<const StorageDictionary> storage_owner;
        bool construction_in_progress = true;
    };
    using StorageRegistrations = std::unordered_map<const StorageDictionary *, StorageRegistration>;
    std::unordered_map<const ExternalDictionariesLoader *, std::unordered_map<String, StorageRegistrations>> storages;
};

/// Intentionally process-lifetime: ExternalLoader owns repository objects, so
/// a normal static could otherwise be destroyed before their final teardown.
ActiveDictionaryStorageRepositories & activeDictionaryStorageRepositories()
{
    static auto * registry = new ActiveDictionaryStorageRepositories;
    return *registry;
}

bool isExactReservedBootstrapImage(
    const StorageDictionary & storage,
    const StorageMetadataPtr & metadata,
    const ExternalDictionariesLoader & loader,
    ExternalLoaderDictionaryStorageConfigRepository::ActiveUUIDRepositoryLease & lease)
{
    if (!metadata || !metadata->getBoundUDTReferences())
        return false;

    metadata->validateBoundUDTReferences();
    const auto storage_id = storage.getStorageID();
    const auto & object = metadata->getBoundUDTReferences()->getObject();
    if (!storage_id.hasUUID() || object.kind != UDT::SchemaObjectKind::Dictionary || object.object_uuid != storage_id.uuid
        || storage_id.database_name.empty())
        return false;

    const auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.database_name);
    const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database);
    if (!atomic || object.database_uuid != atomic->getUUID())
        return false;

    lease = ExternalLoaderDictionaryStorageConfigRepository::acquireActiveUUIDRepository(loader, toString(storage_id.uuid), &storage);
    if (!lease)
        return false;

    auto & catalog = DatabaseCatalog::instance();
    if (!catalog.hasUUIDMapping(storage_id.uuid))
    {
        lease = {};
        return false;
    }
    const auto [mapped_database, mapped_table] = catalog.tryGetByUUID(storage_id.uuid);
    if (mapped_database || mapped_table)
    {
        lease = {};
        return false;
    }
    return true;
}

}

ExternalLoaderDictionaryStorageConfigRepository::ExternalLoaderDictionaryStorageConfigRepository(const StorageDictionary & dictionary_storage_)
    : dictionary_storage(dictionary_storage_)
{
    const auto storage_id = dictionary_storage.getStorageID();
    if (storage_id.uuid == UUIDHelpers::Nil)
        return;

    registered_loader = &dictionary_storage.getContext()->getExternalDictionariesLoader();
    registered_uuid = toString(storage_id.uuid);
    auto & registry = activeDictionaryStorageRepositories();
    std::lock_guard lock(registry.mutex);
    auto & registration = registry.storages[registered_loader][registered_uuid][&dictionary_storage];
    ++registration.repository_count;
    if (auto owner = std::dynamic_pointer_cast<const StorageDictionary>(dictionary_storage.weak_from_this().lock()))
    {
        registration.storage_owner = std::move(owner);
        registration.construction_in_progress = false;
    }
}

ExternalLoaderDictionaryStorageConfigRepository::~ExternalLoaderDictionaryStorageConfigRepository()
{
    if (!registered_loader)
        return;

    auto & registry = activeDictionaryStorageRepositories();
    std::lock_guard lock(registry.mutex);
    auto loader_it = registry.storages.find(registered_loader);
    if (loader_it == registry.storages.end())
        return;
    auto uuid_it = loader_it->second.find(registered_uuid);
    if (uuid_it == loader_it->second.end())
        return;

    auto storage_it = uuid_it->second.find(&dictionary_storage);
    if (storage_it == uuid_it->second.end())
        return;
    if (--storage_it->second.repository_count == 0)
        uuid_it->second.erase(storage_it);
    if (uuid_it->second.empty())
        loader_it->second.erase(uuid_it);
    if (loader_it->second.empty())
        registry.storages.erase(loader_it);
}

ExternalLoaderDictionaryStorageConfigRepository::ActiveUUIDRepositoryLease
ExternalLoaderDictionaryStorageConfigRepository::acquireActiveUUIDRepository(
    const ExternalDictionariesLoader & loader, const std::string & uuid, const StorageDictionary * expected_storage)
{
    auto & registry = activeDictionaryStorageRepositories();
    std::unique_lock lock(registry.mutex);
    const auto loader_it = registry.storages.find(&loader);
    if (loader_it == registry.storages.end())
        return {};
    const auto uuid_it = loader_it->second.find(uuid);
    if (uuid_it == loader_it->second.end() || uuid_it->second.size() != 1)
        return {};

    const auto & [storage, registration] = *uuid_it->second.begin();
    auto storage_owner = registration.storage_owner.lock();
    /// The empty UUID mapping also exists after DETACH/drop. Repository
    /// teardown is intentionally separate and may lag behind that mapping
    /// transition, so mere registration is not a bootstrap capability.
    /// Require the one exact repository-owning storage to still be in its
    /// pre-publication/live state; duplicate owners fail closed as split brain.
    if (!storage || (expected_storage && storage != expected_storage) || registration.repository_count != 1
        || (!registration.construction_in_progress && !storage_owner) || storage->is_dropped.load(std::memory_order_acquire)
        || storage->is_detached.load(std::memory_order_acquire) || storage->is_being_restarted.load(std::memory_order_acquire))
        return {};

    return ActiveUUIDRepositoryLease(std::move(lock), std::move(storage_owner), storage);
}

bool ExternalLoaderDictionaryStorageConfigRepository::bindActiveUUIDRepositoryStorage(
    const ExternalDictionariesLoader & loader, const std::string & uuid, const std::shared_ptr<StorageDictionary> & storage)
{
    if (!storage)
        return false;

    auto & registry = activeDictionaryStorageRepositories();
    std::lock_guard lock(registry.mutex);
    const auto loader_it = registry.storages.find(&loader);
    if (loader_it == registry.storages.end())
        return false;
    const auto uuid_it = loader_it->second.find(uuid);
    if (uuid_it == loader_it->second.end())
        return false;
    const auto storage_it = uuid_it->second.find(storage.get());
    if (storage_it == uuid_it->second.end() || uuid_it->second.size() != 1 || storage_it->second.repository_count != 1)
        return false;

    storage_it->second.storage_owner = storage;
    storage_it->second.construction_in_progress = false;
    return true;
}

std::string ExternalLoaderDictionaryStorageConfigRepository::getName() const
{
    return dictionary_storage.getStorageID().getInternalDictionaryName();
}

std::set<std::string> ExternalLoaderDictionaryStorageConfigRepository::getAllLoadablesDefinitionNames()
{
    return { getName() };
}

bool ExternalLoaderDictionaryStorageConfigRepository::exists(const std::string & loadable_definition_name)
{
    return getName() == loadable_definition_name;
}

LoadablesConfigurationPtr ExternalLoaderDictionaryStorageConfigRepository::load(const std::string &)
{
    const auto storage_id = dictionary_storage.getStorageID();
    if (storage_id.uuid != UUIDHelpers::Nil)
    {
        /// This repository is the only config source that owns the exact DDL
        /// StorageDictionary. Gate every config refresh before ExternalLoader
        /// can schedule a periodic/explicit reload. Physical-only metadata is
        /// deliberately a no-op, including the pre-publication bootstrap read.
        const auto context = dictionary_storage.getContext();
        auto metadata_handle = dictionary_storage.getInMemoryMetadataPtr(context, false);
        StorageMetadataPtr metadata = metadata_handle;
        ActiveUUIDRepositoryLease bootstrap_lease;
        const bool is_reserved_bootstrap_image
            = registered_loader && isExactReservedBootstrapImage(dictionary_storage, metadata, *registered_loader, bootstrap_lease);
        if (!is_reserved_bootstrap_image)
        {
            UDT::assertAuthorityStorageNewOperationAllowed(dictionary_storage, metadata, UDT::AuthorityQuarantineOperationKind::DDL);

            /// Test-only pause after the live-image admission and before the
            /// repository returns its configuration. At this point no UDT
            /// registry lease is held; the caller's config-reader lock keeps
            /// the exact repository alive while concurrent reload and
            /// detach/drop paths exercise their normal lock ordering.
            if (metadata && metadata->getBoundUDTReferences() && FailPointInjection::hasAnyFailPointBeenRegistered()
                && CurrentThread::getQueryId().starts_with(dictionary_repository_failpoint_query_id_prefix))
                FailPointInjection::pauseFailPoint(FailPoints::udt_dictionary_repository_pause_after_live_admission);
        }
    }
    return dictionary_storage.getConfiguration();
}

}
