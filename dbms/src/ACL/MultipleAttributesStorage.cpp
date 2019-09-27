#include <ACL/MultipleAttributesStorage.h>
#include <string>
#include <functional>


namespace DB
{
namespace
{
    extern const size_t CACHE_MAX_SIZE = 128;
}


size_t MultipleAttributesStorage::NameAndTypeHash::operator()(const NameAndType & pr) const
{
    return std::hash<String>()(pr.first) + std::hash<const Type *>()(pr.second);
}


MultipleAttributesStorage::MultipleAttributesStorage(std::vector<std::unique_ptr<Storage>> nested_storages_, size_t index_of_nested_storage_for_insertion_)
    : nested_storages(std::move(nested_storages_)), nested_storage_for_insertion(nested_storages[index_of_nested_storage_for_insertion_].get()),
      names_and_types_cache(CACHE_MAX_SIZE), ids_cache(CACHE_MAX_SIZE)
{
}


MultipleAttributesStorage::~MultipleAttributesStorage()
{
}


const String & MultipleAttributesStorage::getStorageName() const
{
    static const String storage_name = "Multiple";
    return storage_name;
}


std::vector<UUID> MultipleAttributesStorage::findPrefixedImpl(const String & prefix, const Type & type) const
{
    std::vector<UUID> all_ids;
    for (const auto & nested_storage : nested_storages)
    {
        auto ids = nested_storage->findPrefixed(prefix, type);
        all_ids.insert(all_ids.end(), std::make_move_iterator(ids.begin()), std::make_move_iterator(ids.end()));
    }
    return all_ids;
}


std::optional<UUID> MultipleAttributesStorage::findImpl(const String & name, const Type & type) const
{
    std::unique_lock lock{mutex};
    auto from_cache = names_and_types_cache.get({name, &type});
    if (from_cache)
    {
        const auto [id, storage] = *from_cache;
        lock.unlock();
        if (id == storage->find(name, type))
            return id;
    }
    else
        lock.unlock();

    for (const auto & nested_storage : nested_storages)
    {
        auto id = nested_storage->find(name, type);
        if (id)
        {
            lock.lock();
            names_and_types_cache.set({name, &type}, std::make_shared<IDAndStorage>(*id, nested_storage.get()));
            ids_cache.set(*id, std::make_shared<Storage *>(nested_storage.get()));
            return *id;
        }
    }

    return {};
}


bool MultipleAttributesStorage::existsImpl(const UUID & id) const
{
    return findStorageByID(id) != nullptr;
}


IAttributesStorage * MultipleAttributesStorage::findStorageByID(const UUID & id) const
{
    std::unique_lock lock{mutex};
    auto from_cache = ids_cache.get(id);
    if (from_cache)
    {
        auto * storage = *from_cache;
        lock.unlock();
        if (storage->exists(id))
            return storage;
    }
    else
        lock.unlock();

    for (const auto & nested_storage : nested_storages)
    {
        if (nested_storage->exists(id))
        {
            lock.lock();
            ids_cache.set(id, std::make_shared<Storage *>(nested_storage.get()));
            return nested_storage.get();
        }
    }

    return nullptr;
}


IAttributesStorage & MultipleAttributesStorage::getStorageByID(const UUID & id) const
{
    auto * storage = findStorageByID(id);
    if (storage)
        return *storage;
    throwNotFound(id);
}

AttributesPtr MultipleAttributesStorage::readImpl(const UUID & id) const
{
    return getStorageByID(id).read(id);
}


UUID MultipleAttributesStorage::insertImpl(const IAttributes & attrs)
{
    auto id = nested_storage_for_insertion->insert(attrs);

    std::lock_guard lock{mutex};
    names_and_types_cache.set({attrs.name, &attrs.getType()}, std::make_shared<IDAndStorage>(id, nested_storage_for_insertion));
    ids_cache.set(id, std::make_shared<Storage *>(nested_storage_for_insertion));

    return id;
}


void MultipleAttributesStorage::removeImpl(const UUID & id)
{
    getStorageByID(id).remove(id);
}


void MultipleAttributesStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    getStorageByID(id).update(id, update_func);
}


IAttributesStorage::SubscriptionPtr MultipleAttributesStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const
{
    auto storage = findStorageByID(id);
    if (!storage)
        return nullptr;
    return storage->subscribeForChanges(id, on_changed);
}


class MultipleAttributesStorage::SubscriptionForNew : public IAttributesStorage::Subscription
{
public:
    SubscriptionForNew(std::vector<SubscriptionPtr> nested_subscriptions_) : nested_subscriptions(std::move(nested_subscriptions_)) {}
    ~SubscriptionForNew() override {}

private:
    std::vector<SubscriptionPtr> nested_subscriptions;
};


IAttributesStorage::SubscriptionPtr MultipleAttributesStorage::subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const
{
    std::vector<SubscriptionPtr> nested_subscriptions;
    for (const auto & nested_storage : nested_storages)
    {
        auto nested_subscription = nested_storage->subscribeForNew(prefix, type, on_new);
        if (nested_subscription)
            nested_subscriptions.emplace_back(std::move(nested_subscription));
    }

    if (nested_subscriptions.empty())
        return nullptr;

    return std::make_unique<SubscriptionForNew>(std::move(nested_subscriptions));
}
}
