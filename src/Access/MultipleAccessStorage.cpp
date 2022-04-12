#include <Access/MultipleAccessStorage.h>
#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <base/range.h>
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

AccessEntityPtr MultipleAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    if (auto storage = findStorage(id))
        return storage->read(id, throw_if_not_exists);

    if (throw_if_not_exists)
        throwNotFound(id);
    else
        return nullptr;
}


std::optional<String> MultipleAccessStorage::readNameImpl(const UUID & id, bool throw_if_not_exists) const
{
    if (auto storage = findStorage(id))
        return storage->readName(id, throw_if_not_exists);

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


std::optional<UUID> MultipleAccessStorage::insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists)
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

    auto id = storage_for_insertion->insert(entity, replace_if_exists, throw_if_exists);
    if (id)
    {
        std::lock_guard lock{mutex};
        ids_cache.set(*id, storage_for_insertion);
    }
    return id;
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
                        throw Exception(
                            old_entity->formatTypeWithName() + ": cannot rename to " + backQuote(new_entity->getName()) + " because "
                                + new_entity->formatTypeWithName() + " already exists in " + storage->getStorageName(),
                            ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
                    }
                }
            }
        }
    }

    return storage_for_updating->update(id, update_func, throw_if_not_exists);
}


scope_guard MultipleAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    auto storage = findStorage(id);
    if (!storage)
        return {};
    return storage->subscribeForChanges(id, handler);
}


bool MultipleAccessStorage::hasSubscription(const UUID & id) const
{
    auto storages = getStoragesInternal();
    for (const auto & storage : *storages)
    {
        if (storage->hasSubscription(id))
            return true;
    }
    return false;
}


scope_guard MultipleAccessStorage::subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const
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


bool MultipleAccessStorage::hasSubscription(AccessEntityType type) const
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

    std::vector<std::pair<StoragePtr, scope_guard>> added_subscriptions[static_cast<size_t>(AccessEntityType::MAX)];
    std::vector<scope_guard> removed_subscriptions;

    for (auto type : collections::range(AccessEntityType::MAX))
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

    for (auto type : collections::range(AccessEntityType::MAX))
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

    for (auto type : collections::range(AccessEntityType::MAX))
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
}


std::optional<UUID>
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
        auto id = storage->authenticate(credentials, address, external_authenticators,
                                        (throw_if_user_not_exists && is_last_storage),
                                        allow_no_password, allow_plaintext_password);
        if (id)
        {
            std::lock_guard lock{mutex};
            ids_cache.set(*id, storage);
            return id;
        }
    }

    if (throw_if_user_not_exists)
        throwNotFound(AccessEntityType::USER, credentials.getUserName());
    else
        return std::nullopt;
}

}
