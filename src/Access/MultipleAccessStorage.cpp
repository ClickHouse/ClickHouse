#include <Access/MultipleAccessStorage.h>
#include <Common/Exception.h>
#include <ext/range.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
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
    /// It's better to remove the storages in the reverse order because they could depend on each other somehow.
    const auto storages = getStoragesPtr();
    for (const auto & storage : *storages | boost::adaptors::reversed)
    {
        removeStorage(storage);
    }
}

void MultipleAccessStorage::setStorages(const std::vector<StoragePtr> & storages)
{
    std::unique_lock lock{mutex};
    nested_storages = std::make_shared<const Storages>(storages);
    ids_cache.reset();
    updateSubscriptionsToNestedStorages(lock);
}

void MultipleAccessStorage::addStorage(const StoragePtr & new_storage)
{
    std::unique_lock lock{mutex};
    if (boost::range::find(*nested_storages, new_storage) != nested_storages->end())
        return;
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    new_storages->push_back(new_storage);
    nested_storages = new_storages;
    updateSubscriptionsToNestedStorages(lock);
}

void MultipleAccessStorage::removeStorage(const StoragePtr & storage_to_remove)
{
    std::unique_lock lock{mutex};
    auto it = boost::range::find(*nested_storages, storage_to_remove);
    if (it == nested_storages->end())
        return;
    size_t index = it - nested_storages->begin();
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    new_storages->erase(new_storages->begin() + index);
    nested_storages = new_storages;
    ids_cache.reset();
    updateSubscriptionsToNestedStorages(lock);
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


std::optional<UUID> MultipleAccessStorage::findImpl(EntityType type, const String & name) const
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


std::vector<UUID> MultipleAccessStorage::findAllImpl(EntityType type) const
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


bool MultipleAccessStorage::existsImpl(const UUID & id) const
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

AccessEntityPtr MultipleAccessStorage::readImpl(const UUID & id) const
{
    return getStorage(id)->read(id);
}


String MultipleAccessStorage::readNameImpl(const UUID & id) const
{
    return getStorage(id)->readName(id);
}


bool MultipleAccessStorage::canInsertImpl(const AccessEntityPtr & entity) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->canInsert(entity))
            return true;
    }
    return false;
}


UUID MultipleAccessStorage::insertImpl(const AccessEntityPtr & entity, bool replace_if_exists)
{
    auto storages = getStoragesInternal();

    std::shared_ptr<IAccessStorage> storage_for_insertion;
    for (const auto & storage : *storages)
    {
        if (storage->canInsert(entity) ||
            storage->find(entity->getType(), entity->getName()))
        {
            storage_for_insertion = storage;
            break;
        }
    }

    if (!storage_for_insertion)
        throw Exception("Not found a storage to insert " + entity->outputTypeAndName(), ErrorCodes::ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND);

    auto id = replace_if_exists ? storage_for_insertion->insertOrReplace(entity) : storage_for_insertion->insert(entity);
    std::lock_guard lock{mutex};
    ids_cache.set(id, storage_for_insertion);
    return id;
}


void MultipleAccessStorage::removeImpl(const UUID & id)
{
    getStorage(id)->remove(id);
}


void MultipleAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    auto storage_for_updating = getStorage(id);

    /// If the updating involves renaming check that the renamed entity will be accessible by name.
    auto storages = getStoragesInternal();
    if ((storages->size() > 1) && (storages->front() != storage_for_updating))
    {
        auto old_entity = storage_for_updating->read(id);
        auto new_entity = update_func(old_entity);
        if (new_entity->getName() != old_entity->getName())
        {
            for (const auto & storage : *storages)
            {
                if (storage == storage_for_updating)
                    break;
                if (storage->find(new_entity->getType(), new_entity->getName()))
                {
                    throw Exception(
                        old_entity->outputTypeAndName() + ": cannot rename to " + backQuote(new_entity->getName()) + " because "
                            + new_entity->outputTypeAndName() + " already exists in " + storage->getStorageName(),
                        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
                }
            }
        }
    }

    storage_for_updating->update(id, update_func);
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    auto storage = findStorage(id);
    if (!storage)
        return {};
    return storage->subscribeForChanges(id, handler);
}


bool MultipleAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->hasSubscription(id))
            return true;
    }
    return false;
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::unique_lock lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());
    if (handlers.size() == 1)
        updateSubscriptionsToNestedStorages(lock);

    return [this, type, handler_it]
    {
        std::unique_lock lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
        if (handlers2.empty())
            updateSubscriptionsToNestedStorages(lock2);
    };
}


bool MultipleAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}


/// Updates subscriptions to nested storages.
/// We need the subscriptions to the nested storages if someone has subscribed to us.
/// If any of the nested storages is changed we call our subscribers.
void MultipleAccessStorage::updateSubscriptionsToNestedStorages(std::unique_lock<std::mutex> & lock) const
{
    /// lock is already locked.

    std::vector<std::pair<StoragePtr, ext::scope_guard>> added_subscriptions[static_cast<size_t>(EntityType::MAX)];
    std::vector<ext::scope_guard> removed_subscriptions;

    for (auto type : ext::range(EntityType::MAX))
    {
        auto & handlers = handlers_by_type[static_cast<size_t>(type)];
        auto & subscriptions = subscriptions_to_nested_storages[static_cast<size_t>(type)];
        if (handlers.empty())
        {
            /// None has subscribed to us, we need no subscriptions to the nested storages.
            for (auto & subscription : subscriptions | boost::adaptors::map_values)
                removed_subscriptions.push_back(std::move(subscription));
            subscriptions.clear();
        }
        else
        {
            /// Someone has subscribed to us, now we need to have a subscription to each nested storage.
            for (auto it = subscriptions.begin(); it != subscriptions.end();)
            {
                const auto & storage = it->first;
                auto & subscription = it->second;
                if (boost::range::find(*nested_storages, storage) == nested_storages->end())
                {
                    removed_subscriptions.push_back(std::move(subscription));
                    it = subscriptions.erase(it);
                }
                else
                    ++it;
            }

            for (const auto & storage : *nested_storages)
            {
                if (!subscriptions.count(storage))
                    added_subscriptions[static_cast<size_t>(type)].push_back({storage, nullptr});
            }
        }
    }

    /// Unlock the mutex temporarily because it's much better to subscribe to the nested storages
    /// with the mutex unlocked.
    lock.unlock();
    removed_subscriptions.clear();

    for (auto type : ext::range(EntityType::MAX))
    {
        if (!added_subscriptions[static_cast<size_t>(type)].empty())
        {
            auto on_changed = [this, type](const UUID & id, const AccessEntityPtr & entity)
            {
                Notifications notifications;
                SCOPE_EXIT({ notify(notifications); });
                std::lock_guard lock2{mutex};
                for (const auto & handler : handlers_by_type[static_cast<size_t>(type)])
                    notifications.push_back({handler, id, entity});
            };
            for (auto & [storage, subscription] : added_subscriptions[static_cast<size_t>(type)])
                subscription = storage->subscribeForChanges(type, on_changed);
        }
    }

    /// Lock the mutex again to store added subscriptions to the nested storages.
    lock.lock();
    for (auto type : ext::range(EntityType::MAX))
    {
        if (!added_subscriptions[static_cast<size_t>(type)].empty())
        {
            auto & subscriptions = subscriptions_to_nested_storages[static_cast<size_t>(type)];
            for (auto & [storage, subscription] : added_subscriptions[static_cast<size_t>(type)])
            {
                if (!subscriptions.count(storage) && (boost::range::find(*nested_storages, storage) != nested_storages->end())
                    && !handlers_by_type[static_cast<size_t>(type)].empty())
                {
                    subscriptions.emplace(std::move(storage), std::move(subscription));
                }
            }
        }
    }

    lock.unlock();
    added_subscriptions->clear();
}


UUID MultipleAccessStorage::loginImpl(const String & user_name, const String & password, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        try
        {
            auto id = storage->login(user_name, password, address, external_authenticators, /* replace_exception_with_cannot_authenticate = */ false);
            std::lock_guard lock{mutex};
            ids_cache.set(id, storage);
            return id;
        }
        catch (...)
        {
            if (!storage->find(EntityType::USER, user_name))
            {
                /// The authentication failed because there no users with such name in the `storage`
                /// thus we can try to search in other nested storages.
                continue;
            }
            throw;
        }
    }
    throwNotFound(EntityType::USER, user_name);
}


UUID MultipleAccessStorage::getIDOfLoggedUserImpl(const String & user_name) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        try
        {
            auto id = storage->getIDOfLoggedUser(user_name);
            std::lock_guard lock{mutex};
            ids_cache.set(id, storage);
            return id;
        }
        catch (...)
        {
            if (!storage->find(EntityType::USER, user_name))
            {
                /// The authentication failed because there no users with such name in the `storage`
                /// thus we can try to search in other nested storages.
                continue;
            }
            throw;
        }
    }
    throwNotFound(EntityType::USER, user_name);
}

}
