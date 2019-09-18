#include <ACL/MemoryControlAttributesStorage.h>
#include <Common/StringUtils/StringUtils.h>
#include <unordered_set>


namespace DB
{
MemoryControlAttributesStorage::MemoryControlAttributesStorage() {}
MemoryControlAttributesStorage::~MemoryControlAttributesStorage() {}


std::vector<UUID> MemoryControlAttributesStorage::findPrefixed(const String & prefix, const Type & type) const
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


std::optional<UUID> MemoryControlAttributesStorage::find(const String & name, const Type & type) const
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


bool MemoryControlAttributesStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


ControlAttributesPtr MemoryControlAttributesStorage::tryReadImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    const Entry & entry = it->second;
    return entry.attrs;
}


std::pair<UUID, bool> MemoryControlAttributesStorage::insert(const Attributes & attrs)
{
    std::unique_lock lock{mutex};
    size_t namespace_idx = attrs.getType().namespace_idx;
    if (namespace_idx >= all_names.size())
        all_names.resize(namespace_idx + 1);

    /// Check the name is unique.
    auto & names = all_names[namespace_idx];
    auto it = names.find(attrs.name);
    if (it != names.end())
        return {it->second, false};

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
    std::vector<OnNewHandler> notify_list;
    if (namespace_idx < on_new_handlers.size())
    {
        for (auto handler_it = on_new_handlers[namespace_idx].lower_bound(attrs.name); handler_it != on_new_handlers[namespace_idx].end(); ++handler_it)
        {
            const String & prefix = handler_it->first;
            if (!startsWith(attrs.name, prefix))
                break;
            notify_list.push_back(handler_it->second);
        }
    }

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    for (const auto & fn : notify_list)
        fn(id);
    return {id, true};
}


bool MemoryControlAttributesStorage::remove(const UUID & id)
{
    std::unique_lock lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return false;

    Entry & entry = it->second;
    size_t namespace_idx = entry.attrs->getType().namespace_idx;
    auto & names = all_names[namespace_idx];

    /// Prepare list of notifications.
    std::vector<std::pair<OnChangedHandler, AttributesPtr>> notify_list;
    for (auto handler : entry.on_changed_handlers)
        notify_list.emplace_back(handler, nullptr);

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
                notify_list.emplace_back(handler, other_attrs);
        }
    }

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    for (const auto & [fn, param] : notify_list)
        fn(param);
    return true;
}


void MemoryControlAttributesStorage::updateImpl(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func)
{
    std::unique_lock lock{mutex};

    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id, type);

    Entry & entry = it->second;
    entry.attrs->checkIsDerived(type);
    size_t namespace_idx = entry.attrs->getType().namespace_idx;
    auto & names = all_names[namespace_idx];

    auto old_attrs = entry.attrs;
    auto new_attrs = old_attrs->clone();
    update_func(*new_attrs);

    if (*new_attrs == *old_attrs)
        return;

    std::vector<OnNewHandler> new_notify_list;
    if (new_attrs->name != old_attrs->name)
    {
        if (names.count(new_attrs->name))
            throwCannotRenameNewNameIsUsed(
                old_attrs->name, old_attrs->getType(), new_attrs->name, entries.at(names.at(new_attrs->name)).attrs->getType());

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
                    new_notify_list.push_back(handler_it->second);
            }
        }
    }

    entry.attrs = new_attrs;
    std::vector<OnChangedHandler> changed_notify_list{entry.on_changed_handlers.begin(), entry.on_changed_handlers.end()};

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    for (const auto & fn : changed_notify_list)
        fn(new_attrs);
    for (const auto & fn : new_notify_list)
        fn(id);
}


class MemoryControlAttributesStorage::SubscriptionForNew : public IControlAttributesStorage::Subscription
{
public:
    SubscriptionForNew(
        const MemoryControlAttributesStorage * storage_, size_t namespace_idx_, const std::multimap<String, OnNewHandler>::iterator & handler_it_)
        : storage(storage_), namespace_idx(namespace_idx_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForNew() override { storage->removeSubscription(namespace_idx, handler_it); }

private:
    const MemoryControlAttributesStorage * storage;
    size_t namespace_idx;
    std::multimap<String, OnNewHandler>::iterator handler_it;
};


IControlAttributesStorage::SubscriptionPtr MemoryControlAttributesStorage::subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= on_new_handlers.size())
        on_new_handlers.resize(namespace_idx + 1);
    return std::make_unique<SubscriptionForNew>(this, namespace_idx, on_new_handlers[namespace_idx].emplace(prefix, on_new));
}


void MemoryControlAttributesStorage::removeSubscription(size_t namespace_idx, const std::multimap<String, OnNewHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    if (namespace_idx >= on_new_handlers.size())
        return;
    on_new_handlers[namespace_idx].erase(handler_it);
}


class MemoryControlAttributesStorage::SubscriptionForChanges : public IControlAttributesStorage::Subscription
{
public:
    SubscriptionForChanges(
        const MemoryControlAttributesStorage * storage_, const UUID & id_, const std::list<OnChangedHandler>::iterator & handler_it_)
        : storage(storage_), id(id_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForChanges() override { storage->removeSubscription(id, handler_it); }

private:
    const MemoryControlAttributesStorage * storage;
    UUID id;
    std::list<OnChangedHandler>::iterator handler_it;
};


IControlAttributesStorage::SubscriptionPtr MemoryControlAttributesStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    const Entry & entry = it->second;
    return std::make_unique<SubscriptionForChanges>(
        this, id, entry.on_changed_handlers.emplace(entry.on_changed_handlers.end(), on_changed));
}


void MemoryControlAttributesStorage::removeSubscription(const UUID & id, const std::list<OnChangedHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return;
    const Entry & entry = it->second;
    entry.on_changed_handlers.erase(handler_it);
}
}
