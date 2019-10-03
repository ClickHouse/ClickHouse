#if 0
#include <Access/IControlAttributesDriven.h>
#include <Access/IAttributesStorageManager.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


IControlAttributesDriven::IControlAttributesDriven()
{
}


IControlAttributesDriven::IControlAttributesDriven(const UUID & id_, Manager & manager_, Storage * storage_)
    : id(id_), manager(&manager_), storage(storage_)
{
}


IControlAttributesDriven::IControlAttributesDriven(const String & name_, Manager & manager_, Storage * storage_)
    : name(name_), manager(&manager_), storage(storage_)
{
}


IControlAttributesDriven::IControlAttributesDriven(Manager & manager_, Storage * storage_)
    : manager(&manager_), storage(storage_)
{
}


IControlAttributesDriven::IControlAttributesDriven(const IControlAttributesDriven & src) = default;
IControlAttributesDriven & IControlAttributesDriven::operator =(const IControlAttributesDriven & src) = default;
IControlAttributesDriven::IControlAttributesDriven(IControlAttributesDriven && src) = default;
IControlAttributesDriven & IControlAttributesDriven::operator =(IControlAttributesDriven & src) = default;


IControlAttributesDriven::~IControlAttributesDriven()
{
}


UUID IControlAttributesDriven::getID() const
{
    tryGetID();
    if (!id)
        throwNotFound();
    return *id;
}


std::optional<UUID> IControlAttributesDriven::tryGetID() const
{
    if (!id)
    {
        if (name)
        {
            if (storage)
                id = storage->find(*name, getType());
            else if (manager)
                std::tie(id, storage) = manager->findInAllStorages(*name, getType());
        }
    }
    return id;
}


IAttributesStorageManager & IControlAttributesDriven::getManager() const
{
    assert(manager);
    return *manager;
}


IAttributesStorageManager * IControlAttributesDriven::tryGetManager() const
{
    return manager;
}


IAttributesStorage & IControlAttributesDriven::getStorage() const
{
    tryGetStorage();
    if (!storage)
        throwNotFound();
    return *storage;
}


IAttributesStorage * IControlAttributesDriven::tryGetStorage() const
{
    if (!storage)
    {
        if (manager)
        {
            if (id)
                storage = manager->findStorage(*id);
            else if (name)
                std::tie(id, storage) = manager->findInAllStorages(*name, getType());
        }
    }
    return storage;
}


AttributesPtr IControlAttributesDriven::getAttributes() const
{
    auto attrs = tryGetAttributes();
    if (!attrs)
        throwNotFound();
    return attrs;
}


AttributesPtr IControlAttributesDriven::tryGetAttributes() const
{
    tryGetStorage();
    tryGetID();
    return (storage && id) ? storage->tryRead(*id, getType()) : nullptr;
}


void IControlAttributesDriven::insert(const AttributesPtr & new_attrs, bool if_not_exists = false)
{
    insertChanges(new_attrs, if_not_exists).apply();
}


IControlAttributesDriven::Changes IControlAttributesDriven::insertChanges(const AttributesPtr & new_attrs, bool if_not_exists = false)
{
    assert (new_attrs->isDerived(getType()));
    assert(id == new_attrs->id);
    if (!isNameUnique(*new_attrs))
    {
        if (if_not_exists)
            return {};
        throw Exception(
            String(type.name) + " " + backQuoteIfNeed(attrs->name) + ": cannot rename to " + backQuoteIfNeed(new_name)
                + " because " + existing_type.name + " " + backQuoteIfNeed(new_name) + " already exists",
            existing_type.error_code_already_exists);
    }
    return {Changes::InsertTag{}, getStorage(), new_attrs, if_not_exists};
}


void IControlAttributesDriven::setName(const String & new_name)
{
    setNameChanges(new_name).apply();
}


IControlAttributesDriven::Changes IControlAttributesDriven::setNameChanges(const String & new_name)
{
    /// Check that the new name is unique through all the storages.
    AttributesPtr attrs_with_same_name;
    if (!isNameUniqueInAllStorages(new_name, attrs_with_same_name))
    {
        const Type & type = getType();
        const Type & existing_type = attrs_with_same_name->getType();
        throw Exception(
            String(type.name) + " " + backQuoteIfNeed(new_name) + ": cannot rename to " + backQuoteIfNeed(new_name)
                + " because " + existing_type.name + " " + backQuoteIfNeed(new_name) + " already exists",
            existing_type.error_code_already_exists);
    }

    auto update_func = [new_name](IAttributes & attrs) { attrs.name = new_name; };
    return {Changes::UpdateTag, getStorage(), getID(), getType(), update_func};
}


bool IControlAttributesDriven::isNameUniqueInAllStorages(const String & new_name, AttributesPtr & attrs_with_same_name)
{
    for (auto * stor : getManager().getAllStorages())
    {
        auto existing_id = stor->find(new_name, getType());
        if (existing_id && (*existing_id != id))
        {
            auto existing_attrs = stor->tryRead(*existing_id);
            if (existing_attrs)
            {
                auto attrs = tryGetAttributes();
                if (attrs)
                {
                    attrs_with_same_name = attrs;
                    return false;
                }
            }
        }
    }
    return true;
}


String IControlAttributesDriven::getName() const
{
    return getAttributes()->name;
}


void IControlAttributesDriven::drop(bool if_exists)
{
    dropChanges(if_exists).apply();
}


IControlAttributesDriven::Changes IControlAttributesDriven::dropChanges(bool if_exists)
{
    tryGetStorage();
    tryGetID();
    if (!storage || !id)
    {
        if (if_exists)
            return {};
        throwNotFound();
    }

    Changes changes{Changes::RemoveTag{}, *storage, *id, getType(), if_exists};

    /// We need to remove references too.
    for (auto * stor : getManager().getAllStorages())
        changes.then(Changes::RemoveReferencesTag{}, *id, *stor);
    return changes;
}


void IControlAttributesDriven::throwNotFound()
{
    const Type & type = getType();
    throw Exception(
        String(type.name) + " " + (name ? backQuoteIfNeed(*name) : (id ? "{" + toString(*id) + "}" : "{}")) + " not found",
        type.error_code_not_found);
}


IControlAttributesDriven::Changes::Changes(InsertTag, Storage & storage, const AttributesPtr & new_attrs, bool if_not_exists)
{
    then(InsertTag{}, storage, new_attrs, if_not_exists);
}


IControlAttributesDriven::Changes::Changes(UpdateTag, Storage & storage, const UUID & id, const Type & type, const std::function<void(IAttributes &)> & update_func, bool if_exists)
{
    then(UpdateTag{}, storage, id, type, update_func, if_exists);
}


IControlAttributesDriven::Changes::Changes(RemoveTag, Storage & storage, const UUID & id, const Type & type, bool if_exists)
{
    then(RemoveTag{}, storage, id, type, if_exists);
}


IControlAttributesDriven::Changes::Changes(RemoveReferencesTag, Storage & storage, const UUID & id)
{
    then(RemoveReferencesTag{}, storage, id);
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::operator=(const Changes & src)
{
    all_changes.clear();
    return then(src);
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::operator=(Changes && src)
{
    all_changes.clear();
    return then(src);
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(const Changes & other)
{
    for (const auto & [other_storage, other_changes] : other.all_changes)
    {
        auto & changes = findStoragePosition(*other_storage);
        changes.insert(changes.end(), other_changes.begin(), other_changes.end());
    }
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(Changes && other)
{
    if (all_changes.empty())
    {
        all_changes.swap(other.all_changes);
        return *this;
    }
    for (auto & [other_storage, other_changes] : other.all_changes)
    {
        auto & changes = findStoragePosition(*other_storage);
        if (changes.empty())
            changes.swap(other_changes);
        else
            changes.insert(changes.end(), std::make_move_iterator(other_changes.begin()), std::make_move_iterator(other_changes.end()));
    }
    other.all_changes.clear();
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(InsertTag, Storage & storage, const AttributesPtr & new_attrs, bool if_not_exists)
{
    auto & change = addChange(storage);
    change.change_type = Storage::ChangeType::INSERT;
    change.insert_attrs = new_attrs;
    change.id = new_attrs->id;
    change.type = new_attrs->getType();
    change.if_not_exists = if_not_exists;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(UpdateTag, Storage & storage, const UUID & id, const Type & type, const std::function<void(IAttributes &)> & update_func, bool if_exists)
{
    auto & change = addChange(storage);
    change.change_type = Storage::ChangeType::UPDATE;
    change.id = id;
    change.type = &type;
    change.update_func = update_func;
    change.if_exists = if_exists;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(RemoveTag, Storage & storage, const UUID & id, const Type & type, bool if_exists)
{
    auto & change = addChange(storage);
    change.change_type = Storage::ChangeType::REMOVE;
    change.id = id;
    change.type = &type;
    change.if_exists = if_exists;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(RemoveReferencesTag, Storage & storage, const UUID & id)
{
    auto & change = addChange(id_and_storage.second);
    change.change_type = Storage::ChangeType::REMOVE_REFERENCES;
    change.id = id;
    return *this;
}


void IControlAttributesDriven::Changes::apply() const
{
    for (const auto & [storage, changes] : all_changes)
        storage->write(changes);
}


IAttributesStorage::Change & IControlAttributesDriven::Changes::addChange(Storage & storage)
{
    auto & changes = findStoragePosition(storage);
    changes.push_back({});
    return changes.back();
}


IAttributesStorage::Changes & IControlAttributesDriven::Changes::findStoragePosition(Storage & storage)
{
    for (auto & [stor, chn] : all_changes)
    {
        if (stor == &storage)
            return chn;
    }
    all_changes.push_back({&storage, {}});
    return all_changes.back().second;
}


/*
void IAttributesStorage::throwCannotInsertNameIsUsed(const String & name, const Type & type, const String & existing_name, const Type & existing_type)
{
    throw Exception(
        String(type.name) + " " + backQuoteIfNeed(name) + ": cannot create because this name is already used by " + existing_type.name + " "
            + backQuoteIfNeed(existing_name),
        existing_type.error_code_already_exists);
}*/
}
#endif
