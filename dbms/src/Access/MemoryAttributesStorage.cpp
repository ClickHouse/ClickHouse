#include <Access/MemoryAttributesStorage.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
MemoryAttributesStorage::MemoryAttributesStorage() {}
MemoryAttributesStorage::~MemoryAttributesStorage() {}


const String & MemoryAttributesStorage::getStorageName() const
{
    static const String storage_name = "Memory";
    return storage_name;
}


std::vector<UUID> MemoryAttributesStorage::findPrefixedImpl(const String & prefix, const Type & type) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= all_names.size())
        return {};
    const auto & names = all_names[namespace_idx];
    std::vector<UUID> result;

    if (prefix.empty())
    {
        result.reserve(names.size());
        for (const auto & name_and_id : names)
        {
            const UUID & id = name_and_id.second;
            result.emplace_back(id);
        }
    }
    else
    {
        for (auto it = names.lower_bound(prefix); it != names.end(); ++it)
        {
            const auto & [name, id] = *it;
            if (!startsWith(name, prefix))
                break;
            result.emplace_back(id);
        }
    }
    return result;
}


std::optional<UUID> MemoryAttributesStorage::findImpl(const String & name, const Type & type) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= all_names.size())
        return {};

    const auto & names = all_names[namespace_idx];
    auto it = names.find(name);
    if (it == names.end())
        return {};

    return it->second;
}


bool MemoryAttributesStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


AttributesPtr MemoryAttributesStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);
    const Entry & entry = it->second;
    return entry.attrs;
}


std::pair<String, const IAttributes::Type *> MemoryAttributesStorage::readNameAndTypeImpl(const UUID &id) const
{
    auto attrs = readImpl(id);
    return {attrs->name, &attrs->getType()};
}


UUID MemoryAttributesStorage::insertImpl(const IAttributes & attrs, bool replace_if_exists)
{
    std::unique_lock lock{mutex};
    size_t namespace_idx = attrs.getType().namespace_idx;
    if (namespace_idx >= all_names.size())
        all_names.resize(namespace_idx + 1);

    /// Check the name is unique.
    auto & names = all_names[namespace_idx];
    auto it = names.find(attrs.name);
    OnChangeNotifications on_change_notifications;
    if (it != names.end())
    {
        if (replace_if_exists)
        {
            for (const auto & handler : entries[it->second].on_changed_handlers)
                on_change_notifications.emplace_back(handler, nullptr);
            entries.erase(it->second);
            names.erase(it);
        }
        else
        {
            auto existing = entries.at(it->second).attrs;
            throwNameCollisionCannotInsert(attrs.name, attrs.getType(), existing->getType());
        }
    }

    /// Generate a new ID.
    UUID id;
    do
    {
        id = generateRandomID();
    }
    while (entries.count(id) /* Nearly impossible */);

    /// Do insertion.
    names[attrs.name] = id;
    entries[id].attrs = attrs.clone();

    /// Prepare list of notifications.
    OnNewNotifications on_new_notifications;
    if (namespace_idx < on_new_handlers.size())
    {
        for (auto handler_it = on_new_handlers[namespace_idx].lower_bound(attrs.name); handler_it != on_new_handlers[namespace_idx].end(); ++handler_it)
        {
            const String & prefix = handler_it->first;
            if (!startsWith(attrs.name, prefix))
                break;
            on_new_notifications.emplace_back(handler_it->second, id);
        }
    }

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    notify(on_change_notifications);
    notify(on_new_notifications);
    return id;
}


void MemoryAttributesStorage::removeImpl(const UUID & id)
{
    std::unique_lock lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);

    Entry & entry = it->second;
    size_t namespace_idx = entry.attrs->getType().namespace_idx;
    auto & names = all_names[namespace_idx];

    /// Prepare list of notifications.
    OnChangeNotifications on_change_notifications;
    for (auto handler : entry.on_changed_handlers)
        on_change_notifications.emplace_back(handler, nullptr);

    /// Do removing.
    names.erase(entry.attrs->name);
    entries.erase(it);

    /// Remove references too (this is an optional part).
    for (auto & other_id_and_entry : entries)
    {
        auto & other_entry = other_id_and_entry.second;
        if (other_entry.attrs->hasReferences(id))
        {
            auto other_attrs = other_entry.attrs->clone();
            other_entry.attrs = other_attrs;
            other_attrs->removeReferences(id);
            for (auto handler : other_entry.on_changed_handlers)
                on_change_notifications.emplace_back(handler, other_attrs);
        }
    }

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    notify(on_change_notifications);
}


void MemoryAttributesStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    std::unique_lock lock{mutex};

    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);

    Entry & entry = it->second;
    size_t namespace_idx = entry.attrs->getType().namespace_idx;
    auto & names = all_names[namespace_idx];

    auto old_attrs = entry.attrs;
    auto new_attrs = old_attrs->clone();
    update_func(*new_attrs);

    if (*new_attrs == *old_attrs)
        return;

    OnNewNotifications on_new_notifications;
    if (new_attrs->name != old_attrs->name)
    {
        if (names.count(new_attrs->name))
        {
            auto existing = entries.at(names.at(new_attrs->name)).attrs;
            throwNameCollisionCannotRename(old_attrs->name, new_attrs->name, old_attrs->getType(), existing->getType());
        }

        names.erase(old_attrs->name);
        names.emplace(new_attrs->name, id);

        if (namespace_idx < on_new_handlers.size())
        {
            for (auto handler_it = on_new_handlers[namespace_idx].lower_bound(new_attrs->name); handler_it != on_new_handlers[namespace_idx].end(); ++handler_it)
            {
                const String & prefix = handler_it->first;
                if (!startsWith(new_attrs->name, prefix))
                    break;
                if (!startsWith(old_attrs->name, prefix))
                    on_new_notifications.emplace_back(handler_it->second, id);
            }
        }
    }

    entry.attrs = new_attrs;
    OnChangeNotifications on_change_notifications;
    for (const auto & handler : entry.on_changed_handlers)
        on_change_notifications.emplace_back(handler, new_attrs);

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    notify(on_change_notifications);
    notify(on_new_notifications);
}


class MemoryAttributesStorage::SubscriptionForNew : public IAttributesStorage::Subscription
{
public:
    SubscriptionForNew(
        const MemoryAttributesStorage * storage_, size_t namespace_idx_, const std::multimap<String, OnNewHandler>::iterator & handler_it_)
        : storage(storage_), namespace_idx(namespace_idx_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForNew() override { storage->removeSubscription(namespace_idx, handler_it); }

private:
    const MemoryAttributesStorage * storage;
    size_t namespace_idx;
    std::multimap<String, OnNewHandler>::iterator handler_it;
};


IAttributesStorage::SubscriptionPtr MemoryAttributesStorage::subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= on_new_handlers.size())
        on_new_handlers.resize(namespace_idx + 1);
    return std::make_unique<SubscriptionForNew>(this, namespace_idx, on_new_handlers[namespace_idx].emplace(prefix, on_new));
}


void MemoryAttributesStorage::removeSubscription(size_t namespace_idx, const std::multimap<String, OnNewHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    if (namespace_idx >= on_new_handlers.size())
        return;
    on_new_handlers[namespace_idx].erase(handler_it);
}


class MemoryAttributesStorage::SubscriptionForChanges : public IAttributesStorage::Subscription
{
public:
    SubscriptionForChanges(
        const MemoryAttributesStorage * storage_, const UUID & id_, const std::list<OnChangedHandler>::iterator & handler_it_)
        : storage(storage_), id(id_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForChanges() override { storage->removeSubscription(id, handler_it); }

private:
    const MemoryAttributesStorage * storage;
    UUID id;
    std::list<OnChangedHandler>::iterator handler_it;
};


IAttributesStorage::SubscriptionPtr MemoryAttributesStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    const Entry & entry = it->second;
    return std::make_unique<SubscriptionForChanges>(
        this, id, entry.on_changed_handlers.emplace(entry.on_changed_handlers.end(), on_changed));
}


void MemoryAttributesStorage::removeSubscription(const UUID & id, const std::list<OnChangedHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return;
    const Entry & entry = it->second;
    entry.on_changed_handlers.erase(handler_it);
}
}
