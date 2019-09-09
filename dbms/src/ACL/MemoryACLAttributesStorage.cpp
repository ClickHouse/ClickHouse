#include <ACL/MemoryACLAttributesStorage.h>
#include <Poco/UUIDGenerator.h>
#include <unordered_set>


namespace DB
{
using Subscription = IACLAttributesStorage::Subscription;
using SubscriptionPtr = IACLAttributesStorage::SubscriptionPtr;

MemoryACLAttributesStorage::MemoryACLAttributesStorage() {}
MemoryACLAttributesStorage::~MemoryACLAttributesStorage() {}


size_t MemoryACLAttributesStorage::indexOfType(ACLAttributesType type)
{
    /// Users and roles use the same name space (it's not allowed to have an user and a role with the same name).
    if (type == ACLAttributesType::USER)
        return static_cast<size_t>(ACLAttributesType::ROLE);
    return static_cast<size_t>(type);
}


std::vector<UUID> MemoryACLAttributesStorage::findAll(ACLAttributesType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    const auto & names_of_type = names[indexOfType(type)];
    result.reserve(names_of_type.size());
    for (const auto & name_and_id : names_of_type)
        result.emplace_back(name_and_id.second);
    return result;
}


std::optional<UUID> MemoryACLAttributesStorage::find(const String & name, ACLAttributesType type) const
{
    std::lock_guard lock{mutex};
    const auto & names_of_type = names[indexOfType(type)];
    auto it = names_of_type.find(name);
    if (it != names_of_type.end())
        return it->second;
    return {};
}


bool MemoryACLAttributesStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


std::pair<UUID, bool> MemoryACLAttributesStorage::insert(const IACLAttributes & attrs, bool if_not_exists)
{
    std::unique_lock lock{mutex};
    const auto & names_of_type = names[indexOfType(attrs.getType())];
    auto it = names_of_type.find(attrs.name);
    if (it != names_of_type.end())
    {
        if (if_not_exists)
            return {it->second, false};
        throwCannotInsertAlreadyExists(attrs.name, attrs.getType(), entries[it->second].attrs->getType());
    }

    UUID id;
    do
    {
        static Poco::UUIDGenerator generator;
        generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    }
    while (entries.count(id)); /// It's nearly impossible that we need another iteration.

    Entry & entry = entries[id];
    entry.attrs = attrs.clone();
    return {id, true};
}


ACLAttributesPtr MemoryACLAttributesStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    return it->second.attrs;
}


void MemoryACLAttributesStorage::write(const Changes & changes)
{
    std::unique_lock lock{mutex};

    /// Prepare lists of updated and removed attributes.
    std::unordered_map<UUID, std::shared_ptr<IACLAttributes>> updated;
    std::unordered_set<UUID> removed;
    for (const Change & change : changes)
    {
        const auto & id = change.id;
        switch (change.type)
        {
            case Change::Type::UPDATE:
            {
                auto it = updated.find(id);
                if (it == updated.end())
                {
                    bool exists = entries.count(id) && !removed.count(id);
                    if (!exists)
                    {
                        if (entries.count(id))
                            throwNotFound(entries[id].attrs->name, change.attributes_type);
                        else
                            throwNotFound(id, change.attributes_type);
                    }
                    it = updated.try_emplace(id, entries[id].attrs->clone()).first;
                }
                auto new_attrs = it->second;
                change.update_func(*new_attrs);
                break;
            }

            case Change::Type::REMOVE:
            {
                bool exists = entries.count(id) && !removed.count(id);
                if (exists)
                {
                    removed.insert(id);
                    updated.erase(id);
                }
                else if (!change.if_exists)
                {
                    if (entries.count(id))
                        throwNotFound(entries[id].attrs->name, change.attributes_type);
                    else
                        throwNotFound(id, change.attributes_type);
                }
                [[fallthrough]]; /// Also remove references.
            }

            case Change::Type::REMOVE_REFERENCES:
            {
                for (const auto & [target_id, target_entry] : entries)
                {
                    if (!removed.count(target_id))
                    {
                        auto it = updated.find(target_id);
                        if (it != updated.end())
                        {
                            auto new_attrs = it->second;
                            if (new_attrs->hasReferences(id))
                                new_attrs->removeReferences(id);
                        }
                        else
                        {
                            if (target_entry.attrs->hasReferences(id))
                            {
                                it = updated.try_emplace(target_id, target_entry.attrs->clone()).first;
                                auto new_attrs = it->second;
                                new_attrs->removeReferences(id);
                            }
                        }
                    }
                }
                break;
            }

            default:
                __builtin_unreachable();
        }
    }

    /// Checks that updated names are still unique.
    std::unordered_map<String, UUID> names_after_renaming[std::size(names)];
    for (const auto & [id, new_attrs] : updated)
    {
        const Entry & entry = entries[id];
        if (entry.attrs->name != new_attrs->name)
        {
            const auto & names_of_type = names[indexOfType(new_attrs->getType())];
            auto it = names_of_type.find(new_attrs->name);
            if ((it != names_of_type.end()) && (it->second != id) && !removed.count(it->second))
            {
                throwCannotRenameNewNameInUse(entry.attrs->name, new_attrs->name, new_attrs->getType(), entries[it->second].attrs->getType());
            }
            else
            {
                auto & names_after_renaming_of_type = names_after_renaming[indexOfType(new_attrs->getType())];
                it = names_after_renaming_of_type.try_emplace(new_attrs->name, id).first;
                if ((it != names_after_renaming_of_type.end()) && (it->second != id))
                    throwCannotRenameNewNameInUse(entry.attrs->name, new_attrs->name, new_attrs->getType(), entries[it->second].attrs->getType());
            }
        }
    }

    /// Apply changes and built the list of notifications.
    std::vector<std::pair<OnChangedFunction, ACLAttributesPtr>> notify_list;
    for (const auto & [id, new_attrs] : updated)
    {
        Entry & entry = entries[id];
        if (entry.attrs->name != new_attrs->name)
        {
            auto & names_of_type = names[indexOfType(entry.attrs->getType())];
            names_of_type.erase(entry.attrs->name);
            names_of_type.emplace(new_attrs->name, id);
        }
        entry.attrs = new_attrs;
        for (const auto & on_changed_function : entry.on_changed_functions)
            notify_list.push_back({on_changed_function, new_attrs});
    }
    for (const auto & id : removed)
    {
        Entry & entry = entries[id];
        for (const auto & on_changed_function : entry.on_changed_functions)
            notify_list.push_back({on_changed_function, nullptr});
        auto & names_of_type = names[indexOfType(entry.attrs->getType())];
        names_of_type.erase(entry.attrs->name);
        entries.erase(id);
    }

    /// Unlock the `mutex` and notify subscribers.
    lock.unlock();
    for (const auto & [fn, param] : notify_list)
        fn(param);
}


class MemoryACLAttributesStorage::SubscriptionImpl : public Subscription
{
public:
    SubscriptionImpl(
        const MemoryACLAttributesStorage * storage_, const UUID & id_, const std::list<OnChangedFunction>::iterator & functions_it_)
        : storage(storage_), id(id_), functions_it(functions_it_)
    {
    }
    ~SubscriptionImpl() override { storage->removeSubscription(id, functions_it); }

private:
    const MemoryACLAttributesStorage * storage;
    UUID id;
    std::list<OnChangedFunction>::iterator functions_it;
};


SubscriptionPtr MemoryACLAttributesStorage::subscribeForChangesImpl(const UUID & id, const OnChangedFunction & on_changed) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    auto & functions = it->second.on_changed_functions;
    return std::make_unique<SubscriptionImpl>(this, id, functions.emplace(functions.end(), on_changed));
}


void MemoryACLAttributesStorage::removeSubscription(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return;
    auto & functions = it->second.on_changed_functions;
    functions.erase(functions_it);
}
}
