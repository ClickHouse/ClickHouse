#include <ACL/MemoryACLAttributesStorage.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
using Status = IACLAttributesStorage::Status;
using Subscription = IACLAttributesStorage::Subscription;
using SubscriptionPtr = IACLAttributesStorage::SubscriptionPtr;


MemoryACLAttributesStorage::MemoryACLAttributesStorage()
{
    static_assert(static_cast<size_t>(IACLAttributes::Type::USER) < MAX_TYPE);
    static_assert(static_cast<size_t>(IACLAttributes::Type::ROLE) < MAX_TYPE);
    static_assert(static_cast<size_t>(IACLAttributes::Type::QUOTA) < MAX_TYPE);
    static_assert(static_cast<size_t>(IACLAttributes::Type::ROW_FILTER_POLICY) < MAX_TYPE);
}


MemoryACLAttributesStorage::~MemoryACLAttributesStorage() {}


std::vector<UUID> MemoryACLAttributesStorage::findAll(IACLAttributes::Type type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    const auto & names_of_type = names[static_cast<size_t>(type)];
    result.reserve(names_of_type.size());
    for (const auto & name_and_id : names_of_type)
        result.emplace_back(name_and_id.second);
    return result;
}


UUID MemoryACLAttributesStorage::find(const String & name, IACLAttributes::Type type) const
{
    std::lock_guard lock{mutex};
    const auto & names_of_type = names[static_cast<size_t>(type)];
    auto it = names_of_type.find(name);
    if (it != names_of_type.end())
        return it->second;
    return UUID(UInt128(0));
}


bool MemoryACLAttributesStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


Status MemoryACLAttributesStorage::insert(const IACLAttributes & attrs, UUID & id)
{
    std::unique_lock lock{mutex};
    id = UUID(UInt128(0));
    const auto & names_of_type = names[static_cast<size_t>(attrs.getType())];
    auto it = names_of_type.find(attrs.name);
    if (it != names_of_type.end())
        return Status::ALREADY_EXISTS;

    do
    {
        static Poco::UUIDGenerator generator;
        generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    }
    while (entries.count(id));

    Entry & entry = entries[id];
    entry.attrs = attrs.clone();
    return Status::OK;
}


Status MemoryACLAttributesStorage::remove(const UUID & id)
{
    std::unique_lock lock{mutex};
    std::vector<std::pair<OnChangedFunction, ACLAttributesPtr>> notify_list;
    Status result = Status::NOT_FOUND;

    auto it = entries.find(id);
    if (it != entries.end())
    {
        for (const auto & on_changed_function : it->second.on_changed_functions)
            notify_list.push_back({on_changed_function, nullptr});
        const auto & attrs = it->second.attrs;
        names[static_cast<size_t>(attrs->getType())].erase(attrs->name);
        entries.erase(it);
        result = Status::OK;
    }

    for (auto & id_with_entry : entries)
    {
        auto & entry = id_with_entry.second;
        if (entry.attrs->hasReferences(id))
        {
            auto new_attrs = entry.attrs->clone();
            new_attrs->removeReferences(id);
            entry.attrs = new_attrs;
            for (const auto & on_changed_function : entry.on_changed_functions)
                notify_list.push_back({on_changed_function, new_attrs});
        }
    }
    lock.unlock();

    for (const auto & [fn, param] : notify_list)
        fn(param);

    return result;
}


Status MemoryACLAttributesStorage::readImpl(const UUID & id, ACLAttributesPtr & attrs) const
{
    std::lock_guard lock{mutex};
    attrs.reset();
    auto it = entries.find(id);
    if (it == entries.end())
        return Status::NOT_FOUND;
    attrs = it->second.attrs;
    return Status::OK;
}


Status MemoryACLAttributesStorage::writeImpl(const UUID & id, const MakeChangeFunction & make_change)
{
    std::unique_lock lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return Status::NOT_FOUND;
    Entry & entry = it->second;
    auto new_attrs = entry.attrs->clone();
    Status status = make_change(*new_attrs);
    if (status != Status::OK)
        return status;
    if (*entry.attrs == *new_attrs)
        return Status::OK;
    if (entry.attrs->name != new_attrs->name)
    {
        auto & names_of_type = names[static_cast<size_t>(entry.attrs->getType())];
        auto [names_it, inserted] = names_of_type.try_emplace(new_attrs->name, id);
        if (!inserted)
            return Status::ALREADY_EXISTS;
        names_of_type.erase(names_it);
    }
    entry.attrs = new_attrs;
    std::vector<OnChangedFunction> notify_list{entry.on_changed_functions.begin(), entry.on_changed_functions.end()};
    lock.unlock();

    for (const auto & fn : notify_list)
        fn(new_attrs);

    return Status::OK;
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
