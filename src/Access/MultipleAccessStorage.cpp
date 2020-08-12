#include <Access/MultipleAccessStorage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}


MultipleAccessStorage::MultipleAccessStorage(
    const String & storage_name_,
    std::vector<std::unique_ptr<Storage>> nested_storages_)
    : IAccessStorage(storage_name_)
    , nested_storages(std::move(nested_storages_))
    , ids_cache(512 /* cache size */)
{
}


std::optional<UUID> MultipleAccessStorage::findImpl(EntityType type, const String & name) const
{
    for (const auto & nested_storage : nested_storages)
    {
        auto id = nested_storage->find(type, name);
        if (id)
        {
            std::lock_guard lock{ids_cache_mutex};
            ids_cache.set(*id, std::make_shared<Storage *>(nested_storage.get()));
            return *id;
        }
    }
    return {};
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
        if (nested_storage->canInsert(entity) ||
            nested_storage->find(entity->getType(), entity->getName()))
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
    auto & storage_for_updating = getStorage(id);

    /// If the updating involves renaming check that the renamed entity will be accessible by name.
    if ((nested_storages.size() > 1) && (nested_storages.front().get() != &storage_for_updating))
    {
        auto old_entity = storage_for_updating.read(id);
        auto new_entity = update_func(old_entity);
        if (new_entity->getName() != old_entity->getName())
        {
            for (const auto & nested_storage : nested_storages)
            {
                if (nested_storage.get() == &storage_for_updating)
                    break;
                if (nested_storage->find(new_entity->getType(), new_entity->getName()))
                {
                    throw Exception(
                        old_entity->outputTypeAndName() + ": cannot rename to " + backQuote(new_entity->getName()) + " because "
                            + new_entity->outputTypeAndName() + " already exists in " + nested_storage->getStorageName(),
                        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
                }
            }
        }
    }

    storage_for_updating.update(id, update_func);
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
