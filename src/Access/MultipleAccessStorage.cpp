#include <Access/MultipleAccessStorage.h>
#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <base/insertAtEnd.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND;
    extern const int ACCESS_ENTITY_NOT_FOUND;
}

using Storage = IAccessStorage;
using StoragePtr = std::shared_ptr<Storage>;
using ConstStoragePtr = std::shared_ptr<const Storage>;
using Storages = std::vector<StoragePtr>;


MultipleAccessStorage::MultipleAccessStorage(const String & storage_name_)
    : IAccessStorage(storage_name_)
    , nested_storages(std::make_shared<Storages>())
    , ids_cache(512 /* cache size */)
{
}

MultipleAccessStorage::~MultipleAccessStorage()
{
    try
    {
        MultipleAccessStorage::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void MultipleAccessStorage::shutdown()
{
    /// It's better to shutdown the storages in the reverse order because they could depend on each other somehow.
    const auto storages = getStoragesPtr();
    for (const auto & storage : *storages | boost::adaptors::reversed)
    {
        storage->shutdown();
    }
}

void MultipleAccessStorage::setStorages(const std::vector<StoragePtr> & storages)
{
    std::lock_guard lock{mutex};
    nested_storages = std::make_shared<const Storages>(storages);
    ids_cache.clear();
}

void MultipleAccessStorage::addStorage(const StoragePtr & new_storage)
{
    std::lock_guard lock{mutex};
    if (boost::range::find(*nested_storages, new_storage) != nested_storages->end())
        return;
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    new_storages->push_back(new_storage);
    nested_storages = new_storages;
}

void MultipleAccessStorage::removeStorage(const StoragePtr & storage_to_remove)
{
    std::lock_guard lock{mutex};
    auto it = boost::range::find(*nested_storages, storage_to_remove);
    if (it == nested_storages->end())
        return;
    size_t index = it - nested_storages->begin();
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    new_storages->erase(new_storages->begin() + index);
    nested_storages = new_storages;
    ids_cache.clear();
}

void MultipleAccessStorage::removeAllStorages()
{
    /// It's better to remove the storages in the reverse order because they could depend on each other somehow.
    const auto storages = getStoragesPtr();
    for (const auto & storage : *storages | boost::adaptors::reversed)
    {
        removeStorage(storage);
    }
}

std::vector<StoragePtr> MultipleAccessStorage::getStorages()
{
    return *getStoragesPtr();
}

std::vector<ConstStoragePtr> MultipleAccessStorage::getStorages() const
{
    auto storages = getStoragesInternal();
    std::vector<ConstStoragePtr> res;
    res.reserve(storages->size());
    boost::range::copy(*storages, std::back_inserter(res));
    return res;
}

std::shared_ptr<const Storages> MultipleAccessStorage::getStoragesPtr()
{
    return getStoragesInternal();
}

std::shared_ptr<const Storages> MultipleAccessStorage::getStoragesInternal() const
{
    std::lock_guard lock{mutex};
    return nested_storages;
}


std::optional<UUID> MultipleAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        auto id = storage->find(type, name);
        if (id)
        {
            std::lock_guard lock{mutex};
            ids_cache.set(*id, storage);
            return id;
        }
    }
    return {};
}


std::vector<UUID> MultipleAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::vector<UUID> all_ids;
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        auto ids = storage->findAll(type);
        all_ids.insert(all_ids.end(), std::make_move_iterator(ids.begin()), std::make_move_iterator(ids.end()));
    }
    return all_ids;
}


bool MultipleAccessStorage::exists(const UUID & id) const
{
    return findStorage(id) != nullptr;
}


StoragePtr MultipleAccessStorage::findStorage(const UUID & id)
{
    StoragePtr from_cache;
    {
        std::lock_guard lock{mutex};
        from_cache = ids_cache.get(id);
    }
    if (from_cache && from_cache->exists(id))
        return from_cache;

    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->exists(id))
        {
            std::lock_guard lock{mutex};
            ids_cache.set(id, storage);
            return storage;
        }
    }

    return nullptr;
}


ConstStoragePtr MultipleAccessStorage::findStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->findStorage(id);
}


StoragePtr MultipleAccessStorage::getStorage(const UUID & id)
{
    auto storage = findStorage(id);
    if (storage)
        return storage;
    throwNotFound(id);
}


ConstStoragePtr MultipleAccessStorage::getStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->getStorage(id);
}

StoragePtr MultipleAccessStorage::findStorageByName(const String & storage_name)
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->getStorageName() == storage_name)
            return storage;
    }

    return nullptr;
}


ConstStoragePtr MultipleAccessStorage::findStorageByName(const String & storage_name) const
{
    return const_cast<MultipleAccessStorage *>(this)->findStorageByName(storage_name);
}


StoragePtr MultipleAccessStorage::getStorageByName(const String & storage_name)
{
    auto storage = findStorageByName(storage_name);
    if (storage)
        return storage;

    throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access storage with name {} is not found", storage_name);
}


ConstStoragePtr MultipleAccessStorage::getStorageByName(const String & storage_name) const
{
    return const_cast<MultipleAccessStorage *>(this)->getStorageByName(storage_name);
}

StoragePtr MultipleAccessStorage::findExcludingStorage(AccessEntityType type, const String & name, DB::MultipleAccessStorage::StoragePtr exclude) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage == exclude)
            continue;

        if (storage->find(type, name))
            return storage;
    }

    return nullptr;
}

void MultipleAccessStorage::moveAccessEntities(const std::vector<UUID> & ids, const String & source_storage_name, const String & destination_storage_name)
{
    auto source_storage = getStorageByName(source_storage_name);
    auto destination_storage = getStorageByName(destination_storage_name);

    auto to_move = source_storage->read(ids);
    bool need_rollback = false;

    try
    {
        source_storage->remove(ids); // NOLINT
        need_rollback = true;
        destination_storage->insert(to_move, ids);
    }
    catch (Exception & e)
    {
        String message;

        bool need_comma = false;
        for (const auto & entity : to_move)
        {
            if (std::exchange(need_comma, true))
                message += ", ";

            message += entity->formatTypeWithName();
        }

        e.addMessage("while moving {} from {} to {}", message, source_storage_name, destination_storage_name);

        if (need_rollback)
            source_storage->insert(to_move, ids);

        throw;
    }
}

AccessEntityPtr MultipleAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    if (auto storage = findStorage(id))
        return storage->read(id, throw_if_not_exists);

    if (throw_if_not_exists)
        throwNotFound(id);
    else
        return nullptr;
}


std::optional<std::pair<String, AccessEntityType>> MultipleAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    if (auto storage = findStorage(id))
        return storage->readNameWithType(id, throw_if_not_exists);

    if (throw_if_not_exists)
        throwNotFound(id);
    else
        return std::nullopt;
}


bool MultipleAccessStorage::isReadOnly() const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (!storage->isReadOnly())
            return false;
    }
    return true;
}


bool MultipleAccessStorage::isReadOnly(const UUID & id) const
{
    auto storage = findStorage(id);
    if (storage)
        return storage->isReadOnly(id);
    return false;
}


void MultipleAccessStorage::startPeriodicReloading()
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
        storage->startPeriodicReloading();
}

void MultipleAccessStorage::stopPeriodicReloading()
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
        storage->stopPeriodicReloading();
}

void MultipleAccessStorage::reload(ReloadMode reload_mode)
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
        storage->reload(reload_mode);
}


bool MultipleAccessStorage::insertImpl(const UUID & id, const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    std::shared_ptr<IAccessStorage> storage_for_insertion;

    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (!storage->isReadOnly() || storage->find(entity->getType(), entity->getName()))
        {
            storage_for_insertion = storage;
            break;
        }
    }

    if (!storage_for_insertion)
    {
        throw Exception(
            ErrorCodes::ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND,
            "Could not insert {} because there is no writeable access storage in {}",
            entity->formatTypeWithName(),
            getStorageName());
    }

    if (storage_for_insertion->insert(id, entity, replace_if_exists, throw_if_exists, conflicting_id))
    {
        std::lock_guard lock{mutex};
        ids_cache.set(id, storage_for_insertion);
        return true;
    }

    return false;
}


bool MultipleAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    if (auto storage = findStorage(id))
        return storage->remove(id, throw_if_not_exists);

    if (throw_if_not_exists)
        throwNotFound(id);
    else
        return false;
}


bool MultipleAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    auto storage_for_updating = findStorage(id);
    if (!storage_for_updating)
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    /// If the updating involves renaming check that the renamed entity will be accessible by name.
    auto storages = getStoragesInternal();
    if ((storages->size() > 1) && (storages->front() != storage_for_updating))
    {
        if (auto old_entity = storage_for_updating->tryRead(id))
        {
            auto new_entity = update_func(old_entity);
            if (new_entity->getName() != old_entity->getName())
            {
                for (const auto & storage : *storages)
                {
                    if (storage == storage_for_updating)
                        break;
                    if (storage->find(new_entity->getType(), new_entity->getName()))
                    {
                        throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "{}: cannot rename to {} because {} already exists in {}",
                            old_entity->formatTypeWithName(), backQuote(new_entity->getName()), new_entity->formatTypeWithName(), storage->getStorageName());
                    }
                }
            }
        }
    }

    return storage_for_updating->update(id, update_func, throw_if_not_exists);
}


std::optional<AuthResult>
MultipleAccessStorage::authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address,
                                        const ExternalAuthenticators & external_authenticators,
                                        bool throw_if_user_not_exists,
                                        bool allow_no_password, bool allow_plaintext_password) const
{
    auto storages = getStoragesInternal();
    for (size_t i = 0; i != storages->size(); ++i)
    {
        const auto & storage = (*storages)[i];
        bool is_last_storage = (i == storages->size() - 1);
        auto auth_result = storage->authenticate(credentials, address, external_authenticators,
                                        (throw_if_user_not_exists && is_last_storage),
                                        allow_no_password, allow_plaintext_password);
        if (auth_result)
        {
            std::lock_guard lock{mutex};
            ids_cache.set(auth_result->user_id, storage);
            return auth_result;
        }
    }

    if (throw_if_user_not_exists)
        throwNotFound(AccessEntityType::USER, credentials.getUserName());
    else
        return std::nullopt;
}


bool MultipleAccessStorage::isBackupAllowed() const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->isBackupAllowed())
            return true;
    }
    return false;
}


bool MultipleAccessStorage::isRestoreAllowed() const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->isRestoreAllowed())
            return true;
    }
    return false;
}


void MultipleAccessStorage::backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, AccessEntityType type) const
{
    auto storages = getStoragesInternal();
    bool allowed = false;

    for (const auto & storage : *storages)
    {
        if (storage->isBackupAllowed())
        {
            storage->backup(backup_entries_collector, data_path_in_backup, type);
            allowed = true;
        }
    }

    if (!allowed)
        throwBackupNotAllowed();
}

void MultipleAccessStorage::restoreFromBackup(RestorerFromBackup & restorer)
{
    auto storages = getStoragesInternal();

    for (const auto & storage : *storages)
    {
        if (storage->isRestoreAllowed())
        {
            storage->restoreFromBackup(restorer);
            return;
        }
    }

    throwBackupNotAllowed();
}

bool MultipleAccessStorage::containsStorage(std::string_view storage_type) const
{
    auto storages = getStoragesInternal();

    for (const auto & storage : *storages)
    {
        if (storage->getStorageType() == storage_type)
            return true;
    }
    return false;
}
}
