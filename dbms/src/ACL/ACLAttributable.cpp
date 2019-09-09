#include <ACL/ACLAttributable.h>
#include <ACL/ACLAttributesType.h>
#include <ACL/IACLAttributableManager.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
String backQuoteIfNeed(const String & x);

using Operation = ACLAttributable::Operation;


ACLAttributable::ACLAttributable()
{
    atomic_data.store({UInt128{0}, nullptr, std::nullopt});
}


ACLAttributable::ACLAttributable(const UUID & id_, IACLAttributableManager & manager_)
{
    atomic_data.store({id_, &manager_, std::nullopt});
}


ACLAttributable::ACLAttributable(const UUID & id_, IACLAttributableManager & manager_, IACLAttributesStorage & storage_)
{
    atomic_data.store({id_, &manager_, &storage_});
}


ACLAttributable::ACLAttributable(const ACLAttributable & src)
{
    *this = src;
}


ACLAttributable & ACLAttributable::operator =(const ACLAttributable & src)
{
    atomic_data.store(src.atomic_data.load());
    return *this;
}


ACLAttributable::~ACLAttributable()
{
}


UUID ACLAttributable::getID() const
{
    return UUID(atomic_data.load().id);
}


IACLAttributableManager * ACLAttributable::getManager() const
{
    return atomic_data.load().manager;
}


IACLAttributesStorage * ACLAttributable::getStorage() const
{
    return loadData().storage.value();
}


ACLAttributesPtr ACLAttributable::getAttributes() const
{
    auto data = loadData();
    if (!*data.storage)
        return nullptr;
    return (*data.storage)->read(UUID(data.id), getType());
}


ACLAttributesPtr ACLAttributable::getAttributesStrict() const
{
    auto data = loadDataStrict();
    auto attrs = (*data.storage)->read(UUID(data.id), getType());
    if (!attrs)
    {
        const auto & info = ACLAttributesTypeInfo::get(getType());
        throw Exception(info.title + " {" + toString(data.id) + "} not found", info.not_found_error_code);
    }
    return attrs;
}


std::pair<ACLAttributesPtr, IACLAttributableManager *> ACLAttributable::getAttributesWithManagerStrict() const
{
    auto data = loadDataStrict();
    auto attrs = (*data.storage)->read(UUID(data.id), getType());
    if (!attrs)
    {
        const auto & info = ACLAttributesTypeInfo::get(getType());
        throw Exception(info.title + " {" + toString(data.id) + "} not found", info.not_found_error_code);
    }
    return {attrs, data.manager};
}


ACLAttributable::Data ACLAttributable::loadData() const
{
    Data data = atomic_data.load();
    while (!data.storage)
    {
        Data new_data = data;
        new_data = data;
        if (new_data.manager && (new_data.id != UInt128(0)))
            new_data.storage = new_data.manager->findStorage(UUID(data.id));
        else
            new_data.storage = nullptr;
        if (atomic_data.compare_exchange_strong(data, new_data))
            return new_data;
    }
    return data;
}


ACLAttributable::Data ACLAttributable::loadDataStrict() const
{
    Data data = loadData();
    if (!*data.storage)
    {
        const auto & info = ACLAttributesTypeInfo::get(getType());
        throw Exception(info.title + " {" + toString(data.id) + "} not found", info.not_found_error_code);
    }
    return data;
}


bool ACLAttributable::isValid() const
{
    return getAttributes() != nullptr;
}


void ACLAttributable::setName(const String & new_name)
{
    setNameOp(new_name).execute();
}


Operation ACLAttributable::setNameOp(const String & new_name)
{
    /// Check that the new name is unique through all the storages.
    auto data = loadDataStrict();
    for (auto * storage : data.manager->getAllStorages())
    {
        auto existing_id = storage->find(new_name, getType());
        if (existing_id && *existing_id != data.id)
        {
            auto attrs = (*data.storage)->read(UUID(data.id));
            auto existing_attrs = storage->read(*existing_id);
            if (existing_attrs)
            {
                const auto & info = ACLAttributesTypeInfo::get(getType());
                const auto & info_of_existing = ACLAttributesTypeInfo::get(existing_attrs->getType());
                throw Exception(
                    info.title + " " + backQuoteIfNeed(attrs->name) + ": cannot rename to " + backQuoteIfNeed(new_name) + " because " + info_of_existing.title
                        + " " + backQuoteIfNeed(new_name) + " already exists",
                    info_of_existing.already_exists_error_code);
            }
        }
    }

    auto update_func = [new_name](IACLAttributes & attrs) { attrs.name = new_name; };
    return {**data.storage, UUID(data.id), update_func, getType()};
}


String ACLAttributable::getName() const
{
    return getAttributesStrict()->name;
}


String ACLAttributable::getNameOrID() const
{
    auto data = loadData();
    if (*data.storage)
    {
        auto attrs = (*data.storage)->read(UUID(data.id));
        if (attrs)
            return attrs->name;
    }
    if (data.id != 0)
        return "{" + toString(UUID(data.id)) + "}";
    return "{}";
}


void ACLAttributable::drop(bool if_exists)
{
    dropOp(if_exists).execute();
}


Operation ACLAttributable::dropOp(bool if_exists)
{
    auto data = loadDataStrict();
    Operation op{**data.storage, UUID(data.id), Operation::RemoveTag{}, getType(), if_exists};

    /// We need to remove references too.
    for (auto * storage : data.manager->getAllStorages())
        op.then(*storage, UUID(data.id), Operation::RemoveReferencesTag{});
    return op;
}


ACLAttributable::Operation & ACLAttributable::Operation::operator=(const Operation & src)
{
    all_changes.clear();
    return then(src);
}


ACLAttributable::Operation & ACLAttributable::Operation::operator=(Operation && src)
{
    all_changes.clear();
    return then(src);
}


ACLAttributable::Operation::Operation(IACLAttributesStorage & storage, const UUID & id, RemoveTag, ACLAttributesType type, bool if_exists)
{
    then(storage, id, RemoveTag{}, type, if_exists);
}


ACLAttributable::Operation::Operation(IACLAttributesStorage & storage, const UUID & id, RemoveReferencesTag)
{
    then(storage, id, RemoveReferencesTag{});
}


ACLAttributable::Operation & ACLAttributable::Operation::then(const Operation & other)
{
    for (const auto & [other_storage, other_changes] : other.all_changes)
    {
        auto & changes = findPosition(*other_storage);
        changes.insert(changes.end(), other_changes.begin(), other_changes.end());
    }
    return *this;
}


ACLAttributable::Operation & ACLAttributable::Operation::then(Operation && other)
{
    if (all_changes.empty())
    {
        all_changes.swap(other.all_changes);
        return *this;
    }
    for (auto & [other_storage, other_changes] : other.all_changes)
    {
        auto & changes = findPosition(*other_storage);
        if (changes.empty())
            changes.swap(other_changes);
        else
            changes.insert(changes.end(), std::make_move_iterator(other_changes.begin()), std::make_move_iterator(other_changes.end()));
    }
    other.all_changes.clear();
    return *this;
}

ACLAttributable::Operation & ACLAttributable::Operation::then(IACLAttributesStorage & storage, const UUID & id, RemoveTag, ACLAttributesType type, bool if_exists)
{
    auto & change = addChange(storage);
    change.type = Change::Type::REMOVE;
    change.id = id;
    change.if_exists = if_exists;
    change.attributes_type = type;
    return *this;
}

ACLAttributable::Operation & ACLAttributable::Operation::then(IACLAttributesStorage & storage, const UUID & id, RemoveReferencesTag)
{
    auto & change = addChange(storage);
    change.type = Change::Type::REMOVE_REFERENCES;
    change.id = id;
    return *this;
}

void ACLAttributable::Operation::execute() const
{
    for (const auto & [storage, changes] : all_changes)
        storage->write(changes);
}

ACLAttributable::Operation::Changes & ACLAttributable::Operation::findPosition(IACLAttributesStorage & storage)
{
    for (auto & [stg, changes] : all_changes)
    {
        if (stg == &storage)
            return changes;
    }
    all_changes.push_back({&storage, {}});
    return all_changes.back().second;
}

ACLAttributable::Operation::Change & ACLAttributable::Operation::addChange(IACLAttributesStorage & storage)
{
    auto & changes = findPosition(storage);
    changes.push_back({});
    return changes.back();
}

void ACLAttributable::Operation::throwNotFound(const String & name, ACLAttributesType attributes_type)
{
    const auto & info = ACLAttributesTypeInfo::get(attributes_type);
    throw Exception(info.title + " " + backQuoteIfNeed(name) + " not found", info.not_found_error_code);
}
}
