#include <Account/MemoryAccessStorage.h>
#include <Account/RoleAttributes.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
MemoryAccessStorage::MemoryAccessStorage()
{
    static_assert(static_cast<size_t>(Type::ROLE) < MAX_TYPE);
    static_assert(static_cast<size_t>(Type::USER) < MAX_TYPE);
    static_assert(static_cast<size_t>(Type::RLS_POLICY) < MAX_TYPE);
}


MemoryAccessStorage::~MemoryAccessStorage() {}


std::vector<UUID> MemoryAccessStorage::findAll(Type type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    const auto & names_of_type = names[static_cast<size_t>(type)];
    result.reserve(names_of_type.size());
    for (const auto & name_and_id : names_of_type)
        result.emplace_back(name_and_id.second);
    return result;
}


std::optional<UUID> MemoryAccessStorage::find(const String & name, Type type) const
{
    std::lock_guard lock{mutex};
    const auto & names_of_type = names[static_cast<size_t>(type)];
    auto it = names_of_type.find(name);
    if (it != names_of_type.end())
        return it->second;
    return {};
}


bool MemoryAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


UUID MemoryAccessStorage::create(const Attributes & initial_attrs)
{
    std::lock_guard lock{mutex};
    const auto & names_of_type = names[static_cast<size_t>(initial_attrs.type)];
    if (names_of_type.count(initial_attrs.name))
        throwAlreadyExists(initial_attrs.name, initial_attrs.type);

    UUID id;
    do
    {
        static Poco::UUIDGenerator generator;
        generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    }
    while (entries.count(id));

    Entry & entry = entries[id];
    entry.attrs = initial_attrs.clone();
    return id;
}


void MemoryAccessStorage::drop(const UUID & id)
{
    std::unique_lock lock{mutex};
    std::vector<std::tuple<OnChangedFunction, UUID, AttributesPtr>> notify_list;

    auto it = entries.find(id);
    if (it != entries.end())
    {
        for (const auto & on_changed_function : it->second.on_changed_functions)
            notify_list.push_back({on_changed_function, id, nullptr});
        const auto & attrs = it->second.attrs;
        names[static_cast<size_t>(attrs->type)].erase(attrs->name);
        entries.erase(it);
    }

    for (auto & id_with_entry : entries)
    {
        auto & entry = id_with_entry.second;
        if ((entry.attrs->type == Type::ROLE) || (entry.attrs->type == Type::USER))
        {
            const auto & attrs = static_cast<const RoleAttributes &>(*entry.attrs);
            bool update_granted_roles = attrs.granted_roles.count(id);
            bool update_applied_rls_policies = attrs.applied_rls_policies.count(id);
            if (update_granted_roles || update_applied_rls_policies)
            {
                auto new_attrs = std::static_pointer_cast<RoleAttributes>(entry.attrs->clone());
                if (update_granted_roles)
                    new_attrs->granted_roles.erase(id);
                if (update_applied_rls_policies)
                    new_attrs->applied_rls_policies.erase(id);
                entry.attrs = new_attrs;
                for (const auto & on_changed_function : entry.on_changed_functions)
                    notify_list.push_back({on_changed_function, id_with_entry.first, new_attrs});
            }
        }
    }
    lock.unlock();

    for (const auto & [fn, param1, param2] : notify_list)
        fn(param1, param2);
}


IAccessStorage::AttributesPtr MemoryAccessStorage::read(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return getEntry(id).attrs;
}


class MemoryAccessStorage::SubscriptionImpl : public IAccessStorage::Subscription
{
public:
    SubscriptionImpl(
        const MemoryAccessStorage * storage_, const UUID & id_, const std::list<OnChangedFunction>::iterator & functions_it_)
        : storage(storage_), id(id_), functions_it(functions_it_)
    {
    }
    ~SubscriptionImpl() override { storage->removeSubscription(id, functions_it); }

private:
    const MemoryAccessStorage * storage;
    UUID id;
    std::list<OnChangedFunction>::iterator functions_it;
};


void MemoryAccessStorage::removeSubscription(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it != entries.end())
        it->second.on_changed_functions.erase(functions_it);
}


std::pair<IAccessStorage::AttributesPtr, std::unique_ptr<IAccessStorage::Subscription>>
MemoryAccessStorage::readAndSubscribe(const UUID & id, const OnChangedFunction & on_changed) const
{
    std::lock_guard lock{mutex};
    const Entry & entry = getEntry(id);
    return std::make_pair(
        entry.attrs,
        std::make_unique<SubscriptionImpl>(
            this, id, entry.on_changed_functions.emplace(entry.on_changed_functions.end(), on_changed)));
}


void MemoryAccessStorage::write(const UUID & id, const MakeChangeFunction & make_change)
{
    std::unique_lock lock{mutex};
    Entry & entry = getEntry(id);
    auto new_attrs = entry.attrs->clone();
    make_change(*new_attrs);
    if (entry.attrs->isEqual(*new_attrs))
        return;
    if (entry.attrs->name != new_attrs->name)
    {
        auto & names_of_type = names[static_cast<size_t>(entry.attrs->type)];
        auto [names_it, inserted] = names_of_type.try_emplace(new_attrs->name, id);
        if (!inserted)
            throwAlreadyExists(new_attrs->name, entry.attrs->type);
        names_of_type.erase(names_it);
    }
    entry.attrs = new_attrs;
    auto on_changed_functions = entry.on_changed_functions;
    lock.unlock();

    for (const auto & on_changed_function : on_changed_functions)
        on_changed_function(id, new_attrs);
}


MemoryAccessStorage::Entry & MemoryAccessStorage::getEntry(const UUID & id)
{
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);
    return it->second;
}


const MemoryAccessStorage::Entry & MemoryAccessStorage::getEntry(const UUID & id) const
{
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);
    return it->second;
}

}
