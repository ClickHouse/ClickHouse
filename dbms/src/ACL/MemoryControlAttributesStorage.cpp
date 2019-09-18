#include <ACL/MemoryControlAttributesStorage.h>
#include <Common/StringUtils/StringUtils.h>
#include <unordered_set>


namespace DB
{
class MemoryControlAttributesStorage::PreparedChanges
{
public:
    PreparedChanges(MemoryControlAttributesStorage & storage_) : storage(storage_) {}

    void insert(const AttributesPtr & new_attrs, bool if_not_exists)
    {
        const UUID & id = new_attrs->id;
        const Type & type = new_attrs->getType();
        auto it = new_or_updated.find(id);
        if (it != new_or_updated.end())
        {
            auto existing = it->second;
            throwCannotInsertIDIsUsed(id, type, existing->name, existing->getType());
        }

        auto it2 = storage.entries.find(id);
        if (it2 != storage.entries.end())
        {
            auto existing = it2->second.attrs;
            throwCannotInsertIDIsUsed(id, type, existing->name, existing->getType());
        }

        if (!isNameUnique(new_attrs))
        {
            if (if_not_exists)
                return;
            throwCannotInsertNameIsUsed(new_attrs.name, new_attrs.getType(), attrs_with_same_name->name, attrs_with_same_name->getType());
        }

        new_or_updated[id] = new_attrs;
    }

    void remove(const UUID & id, const Type & type, bool if_exists)
    {
        auto it = new_or_updated.find(id);
        if (it != new_or_updated.end())
        {
            auto existing = it->second;
            if (existing->isDerived(type))
            {
                removed.insert(id);
                new_or_updated.erase(it);
                return;
            }
            if (if_exists)
                return;
            throwNotFound(existing->name, type);
        }

        auto it2 = storage.entries.find(id);
        if (it2 != storage.entries.end())
        {
            auto existing = it2->second.attrs;
            if (existing->isDerived(type))
            {
                removed.insert(id);
                return;
            }
            if (if_exists)
                return;
            throwNotFound(existing->name, type);
        }

        if (if_exists)
            return;
        throwNotFound(id, type);
    }

    void update(const UUID & id, const Type & type, const std::function<void(Attributes &)> & update_func, bool if_exists)
    {
        auto it = new_or_updated.find(id);
        if (it != new_or_updated.end())
        {
            auto existing = it->second;
            if (existing->isDerived(type))
            {
                update_func(*existing);
                checkNameIsUnique(*existing);
                return;
            }
            if (if_exists)
                return;
            throwNotFound(existing->name, type);
        }

        auto it2 = storage.entries.find(id);
        if (it2 != storage.entries.end())
        {
            auto existing = it2->second.attrs;
            if (existing->isDerived(type))
            {
                auto cloned = existing->clone();
                new_or_updated[id] = cloned;
                update_func(*cloned);
                checkNameIsUnique(*cloned);
                return;
            }
            if (if_exists)
                return;
            throwNotFound(existing->name, type);
        }

        if (if_exists)
            return;
        throwNotFound(id, type);
    }

    void removeReferences(const UUID & id)
    {
        for (auto & other_id_and_attrs : new_or_updated)
        {
            const auto & other_attrs = other_id_and_attrs.second;
            if (other_attrs->hasReferences(id))
                other_attrs->removeReferences(id);
        }
        for (const auto & other_id_and_entry : storage.entries)
        {
            const auto & other_attrs = other_id_and_entry.second.attrs;
            if (!removed.count(other_attrs->id) && !new_or_updated.count(other_attrs->id) && other_attrs->hasReferences(id))
            {
                auto cloned_attrs = other_attrs->clone();
                new_or_updated.try_emplace(cloned_attrs->id, cloned_attrs);
                cloned_attrs->removeReferences(id);
            }
        }
    }

    void apply()
    {
        for (const auto & id : removed)
        {
            auto entry_it = storage.entries.find(id);
            if (entry_it != storage.entries.end())
            {
                Entry & entry = entry_it->second;
                const auto & attrs = entry.attrs;
                size_t namespace_idx = attrs->getType().namespace_idx;
                if (namespace_idx < storage.all_names.size())
                {
                    auto & names = storage.all_names[namespace_idx];
                    names.erase(attrs->name);
                }
                for (const auto & on_changed_handler : entry.on_changed_handlers)
                    changed_notify_list.push_back({on_changed_handler, nullptr});
                storage.entries.erase(entry_it);
            }
        }

        for (const auto & [id, new_attrs] : new_or_updated)
        {
            size_t namespace_idx = new_attrs->getType().namespace_idx;
            if (namespace_idx >= storage.all_names.size())
                storage.all_names.resize(namespace_idx + 1);
            auto & names = storage.all_names[namespace_idx];
            AttributesPtr old_attrs;

            auto entry_it = storage.entries.find(id);
            if (entry_it == storage.entries.end())
            {
                names[new_attrs->name] = id;
                storage.entries[id].attrs = new_attrs;
            }
            else
            {
                Entry & entry = entry_it->second;
                old_attrs = entry.attrs;
                if (*new_attrs != *old_attrs)
                {
                    if (new_attrs->name != old_attrs->name)
                    {
                        names.erase(old_attrs->name);
                        names.emplace(new_attrs->name, id);
                    }
                    entry.attrs = new_attrs;
                    for (const auto & on_changed_handler : entry.on_changed_handlers)
                        changed_notify_list.push_back({on_changed_handler, new_attrs});
                }
            }

            if (namespace_idx < storage.on_new_handlers.size())
            {
                for (auto it = storage.on_new_handlers[namespace_idx].lower_bound(new_attrs->name); it != storage.on_new_handlers[namespace_idx].end(); ++it)
                {
                    const String & prefix = it->first;
                    if (startsWith(new_attrs->name, prefix) && (!old_attrs || !startsWith(old_attrs->name, prefix)))
                        new_notify_list.push_back({it->second, id});
                }
            }
        }
    }

    void notify()
    {
        for (const auto & [fn, param] : changed_notify_list)
            fn(param);
        for (const auto & [fn, param] : new_notify_list)
            fn(param);
    }

private:
    bool isNameUnique(const Attributes & new_attrs, AttributesPtr & attrs_with_same_name) const
    {
        attrs_with_same_name = nullptr;
        size_t namespace_idx = new_attrs.getType().namespace_idx;
        for (const auto & other_ids_and_attrs : new_or_updated)
        {
            const auto & other_attrs = *other_ids_and_attrs.second;
            if ((other_attrs.name == new_attrs.name) && (other_attrs.getType().namespace_idx == namespace_idx) && (other_attrs.id != new_attrs.id))
            {
                attrs_with_same_name = other_attrs;
                return false;
            }
        }

        if (namespace_idx < storage.all_names.size())
        {
            auto it = storage.all_names[namespace_idx].find(new_attrs.name);
            if (it != storage.all_names[namespace_idx].end())
            {
                const auto & other_id = it->second;
                if ((other_id != new_attrs.id) && !removed.count(other_id) && !new_or_updated.count(other_id))
                {
                    attrs_with_same_name = *storage.entries[other_id].attrs;
                    return false;
                }
            }
        }
        return true;
    }

    void checkNameIsUnique(const Attributes & new_attrs) const
    {
        AttributesPtr attrs_with_same_name;
        if (!isNameUnique(new_attrs, attrs_with_same_name))
            throwCannotRenameNewNameIsUsed(new_attrs.name, new_attrs.getType(), attrs_with_same_name->name, attrs_with_same_name->getType());
    }

    MemoryControlAttributesStorage & storage;
    std::unordered_map<UUID, std::shared_ptr<Attributes>> new_or_updated;
    std::unordered_set<UUID> removed;
    std::vector<std::pair<OnNewHandler, UUID>> new_notify_list;
    std::vector<std::pair<OnChangedHandler, AttributesPtr>> changed_notify_list;
};


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


void MemoryControlAttributesStorage::write(const Changes & changes)
{
    std::unique_lock lock{mutex};
    PreparedChanges prepared_changes(*this);

    for (const Change & change : changes)
    {
        switch (change.change_type)
        {
            case ChangeType::INSERT: prepared_changes.insert(change.id, *change.type); break;
            case ChangeType::REMOVE: prepared_changes.remove(change.id, *change.type, change.if_exists); break;
            case ChangeType::UPDATE: prepared_changes.update(change.id, *change.type, change.update_func, change.if_exists); break;
            case ChangeType::REMOVE_REFERENCES: prepared_changes.removeReferences(change.id); break;
        }
    }

    prepared_changes.apply();

    /// Unlock the `mutex` before notifying the subscribers.
    lock.unlock();
    prepared_changes.notify();
}


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
