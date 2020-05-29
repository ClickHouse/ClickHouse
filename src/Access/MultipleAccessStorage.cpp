#include <Access/MultipleAccessStorage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_FOUND_DUPLICATES;
    extern const int ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND;
}


namespace
{
    template <typename StoragePtrT>
    String joinStorageNames(const std::vector<StoragePtrT> & storages)
    {
        String result;
        for (const auto & storage : storages)
        {
            if (!result.empty())
                result += ", ";
            result += storage->getStorageName();
        }
        return result;
    }
}


MultipleAccessStorage::MultipleAccessStorage(
    std::vector<std::unique_ptr<Storage>> nested_storages_)
    : IAccessStorage(joinStorageNames(nested_storages_))
    , nested_storages(std::move(nested_storages_))
    , ids_cache(512 /* cache size */)
{
}


std::vector<UUID> MultipleAccessStorage::findMultiple(EntityType type, const String & name) const
{
    std::vector<UUID> ids;
    for (const auto & nested_storage : nested_storages)
    {
        auto id = nested_storage->find(type, name);
        if (id)
        {
            std::lock_guard lock{ids_cache_mutex};
            ids_cache.set(*id, std::make_shared<Storage *>(nested_storage.get()));
            ids.push_back(*id);
        }
    }
    return ids;
}


std::optional<UUID> MultipleAccessStorage::findImpl(EntityType type, const String & name) const
{
    auto ids = findMultiple(type, name);
    if (ids.empty())
        return {};
    if (ids.size() == 1)
        return ids[0];

    std::vector<const Storage *> storages_with_duplicates;
    for (const auto & id : ids)
    {
        const auto * storage = findStorage(id);
        if (storage)
            storages_with_duplicates.push_back(storage);
    }

    throw Exception(
        "Found " + outputEntityTypeAndName(type, name) + " in " + std::to_string(ids.size())
            + " storages [" + joinStorageNames(storages_with_duplicates) + "]",
        ErrorCodes::ACCESS_ENTITY_FOUND_DUPLICATES);
}


std::vector<UUID> MultipleAccessStorage::findAllImpl(EntityType type) const
{
    std::vector<UUID> all_ids;
    for (const auto & nested_storage : nested_storages)
    {
        auto ids = nested_storage->findAll(type);
        all_ids.insert(all_ids.end(), std::make_move_iterator(ids.begin()), std::make_move_iterator(ids.end()));
    }
    return all_ids;
}


bool MultipleAccessStorage::existsImpl(const UUID & id) const
{
    return findStorage(id) != nullptr;
}


IAccessStorage * MultipleAccessStorage::findStorage(const UUID & id)
{
    {
        std::lock_guard lock{ids_cache_mutex};
        auto from_cache = ids_cache.get(id);
        if (from_cache)
        {
            auto * storage = *from_cache;
            if (storage->exists(id))
                return storage;
        }
    }

    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->exists(id))
        {
            std::lock_guard lock{ids_cache_mutex};
            ids_cache.set(id, std::make_shared<Storage *>(nested_storage.get()));
            return nested_storage.get();
        }
    }

    return nullptr;
}


const IAccessStorage * MultipleAccessStorage::findStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->findStorage(id);
}


IAccessStorage & MultipleAccessStorage::getStorage(const UUID & id)
{
    auto * storage = findStorage(id);
    if (storage)
        return *storage;
    throwNotFound(id);
}


const IAccessStorage & MultipleAccessStorage::getStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->getStorage(id);
}

void MultipleAccessStorage::addStorage(std::unique_ptr<Storage> nested_storage)
{
    /// Note that IStorage::storage_name is not changed. It is ok as this method
    /// is considered as a temporary solution allowing third-party Arcadia applications
    /// using CH as a library to register their own access storages. Do not remove
    /// this method without providing any alternative :)
    nested_storages.emplace_back(std::move(nested_storage));
}

AccessEntityPtr MultipleAccessStorage::readImpl(const UUID & id) const
{
    return getStorage(id).read(id);
}


String MultipleAccessStorage::readNameImpl(const UUID & id) const
{
    return getStorage(id).readName(id);
}


bool MultipleAccessStorage::canInsertImpl(const AccessEntityPtr & entity) const
{
    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->canInsert(entity))
            return true;
    }
    return false;
}


UUID MultipleAccessStorage::insertImpl(const AccessEntityPtr & entity, bool replace_if_exists)
{
    IAccessStorage * nested_storage_for_insertion = nullptr;
    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->canInsert(entity))
        {
            nested_storage_for_insertion = nested_storage.get();
            break;
        }
    }

    if (!nested_storage_for_insertion)
        throw Exception("Not found a storage to insert " + entity->outputTypeAndName(), ErrorCodes::ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND);

    auto id = replace_if_exists ? nested_storage_for_insertion->insertOrReplace(entity) : nested_storage_for_insertion->insert(entity);
    std::lock_guard lock{ids_cache_mutex};
    ids_cache.set(id, std::make_shared<Storage *>(nested_storage_for_insertion));
    return id;
}


void MultipleAccessStorage::removeImpl(const UUID & id)
{
    getStorage(id).remove(id);
}


void MultipleAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    getStorage(id).update(id, update_func);
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    const auto * storage = findStorage(id);
    if (!storage)
        return {};
    return storage->subscribeForChanges(id, handler);
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    ext::scope_guard subscriptions;
    for (const auto & nested_storage : nested_storages)
        subscriptions.join(nested_storage->subscribeForChanges(type, handler));
    return subscriptions;
}


bool MultipleAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->hasSubscription(id))
            return true;
    }
    return false;
}


bool MultipleAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->hasSubscription(type))
            return true;
    }
    return false;
}
}
