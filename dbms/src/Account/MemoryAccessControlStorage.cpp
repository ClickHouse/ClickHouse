#include <Account/MemoryAccessControlStorage.h>
#include <Poco/UUIDGenerator.h>


namespace DB
{
using Status = IAccessControlStorage::Status;
using Subscription = IAccessControlStorage::Subscription;


MemoryAccessControlStorage::MemoryAccessControlStorage()
{
    static_assert(static_cast<size_t>(ElementType::ROLE) < MAX_TYPE);
    static_assert(static_cast<size_t>(ElementType::USER) < MAX_TYPE);
    static_assert(static_cast<size_t>(ElementType::ROW_LEVEL_SECURITY_POLICY) < MAX_TYPE);
}


MemoryAccessControlStorage::~MemoryAccessControlStorage() {}


std::vector<UUID> MemoryAccessControlStorage::findAll(ElementType type) const
{
    std::lock_guard lock{mutex};
    std::vector<UUID> result;
    const auto & names_of_type = names[static_cast<size_t>(type)];
    result.reserve(names_of_type.size());
    for (const auto & name_and_id : names_of_type)
        result.emplace_back(name_and_id.second);
    return result;
}


UUID MemoryAccessControlStorage::find(const String & name, ElementType type) const
{
    std::lock_guard lock{mutex};
    const auto & names_of_type = names[static_cast<size_t>(type)];
    auto it = names_of_type.find(name);
    if (it != names_of_type.end())
        return it->second;
    return UUID(UInt128(0));
}


bool MemoryAccessControlStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


Status MemoryAccessControlStorage::insert(const Attributes & attrs, UUID & id)
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
    const auto & notify_list_src = on_new_attrs_functions[static_cast<size_t>(attrs.getType())];
    std::vector<OnNewAttributesFunction> notify_list(notify_list_src.begin(), notify_list_src.end());
    lock.unlock();

    for (const auto & fn : notify_list)
        fn(id);

    return Status::OK;
}


Status MemoryAccessControlStorage::remove(const UUID & id)
{
    std::unique_lock lock{mutex};
    std::vector<std::pair<OnChangedFunction, AttributesPtr>> notify_list;
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


Status MemoryAccessControlStorage::read(const UUID & id, AttributesPtr & attrs) const
{
    std::lock_guard lock{mutex};
    attrs.reset();
    auto it = entries.find(id);
    if (it == entries.end())
        return Status::NOT_FOUND;
    attrs = it->second.attrs;
    return Status::OK;
}


Status MemoryAccessControlStorage::write(const UUID & id, const MakeChangeFunction & make_change)
{
    std::unique_lock lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return Status::NOT_FOUND;
    Entry & entry = it->second;
    auto new_attrs = entry.attrs->clone();
    make_change(*new_attrs);
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


class MemoryAccessControlStorage::SubscriptionForNew : public IAccessControlStorage::Subscription
{
public:
    SubscriptionForNew(
        const MemoryAccessControlStorage * storage_, ElementType type_, const std::list<OnNewAttributesFunction>::iterator & functions_it_)
        : storage(storage_), type(type_), functions_it(functions_it_)
    {
    }
    ~SubscriptionForNew() override { storage->removeSubscriptionForNew(type, functions_it); }

private:
    const MemoryAccessControlStorage * storage;
    ElementType type;
    std::list<OnNewAttributesFunction>::iterator functions_it;
};


void MemoryAccessControlStorage::removeSubscriptionForNew(ElementType type, const std::list<OnNewAttributesFunction>::iterator & functions_it) const
{
    std::lock_guard lock{mutex};
    auto & functions = on_new_attrs_functions[static_cast<size_t>(type)];
    functions.erase(functions_it);
}


std::unique_ptr<Subscription> MemoryAccessControlStorage::subscribeForNew(ElementType type, const OnNewAttributesFunction & on_new_attrs) const
{
    std::lock_guard lock{mutex};
    auto & functions = on_new_attrs_functions[static_cast<size_t>(type)];
    return std::make_unique<SubscriptionForNew>(this, type, functions.emplace(functions.end(), on_new_attrs));
}


class MemoryAccessControlStorage::SubscriptionForChanges : public IAccessControlStorage::Subscription
{
public:
    SubscriptionForChanges(
        const MemoryAccessControlStorage * storage_, const UUID & id_, const std::list<OnChangedFunction>::iterator & functions_it_)
        : storage(storage_), id(id_), functions_it(functions_it_)
    {
    }
    ~SubscriptionForChanges() override { storage->removeSubscriptionForChanges(id, functions_it); }

private:
    const MemoryAccessControlStorage * storage;
    UUID id;
    std::list<OnChangedFunction>::iterator functions_it;
};


void MemoryAccessControlStorage::removeSubscriptionForChanges(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return;
    auto & functions = it->second.on_changed_functions;
    functions.erase(functions_it);
}


std::unique_ptr<Subscription> MemoryAccessControlStorage::subscribeForChanges(const UUID & id, const OnChangedFunction & on_changed) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    auto & functions = it->second.on_changed_functions;
    return std::make_unique<SubscriptionForChanges>(this, id, functions.emplace(functions.end(), on_changed));
}
}
