#include <ACL/IControlAttributesDriven.h>
#include <ACL/IControlAttributesStorageManager.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
String backQuoteIfNeed(const String & x);


IControlAttributesDriven::IControlAttributesDriven()
{
    atomic_data.store({UInt128{0}, nullptr, std::nullopt});
}


IControlAttributesDriven::IControlAttributesDriven(const UUID & id_, Manager & manager_)
{
    atomic_data.store({id_, &manager_, std::nullopt});
}


IControlAttributesDriven::IControlAttributesDriven(const UUID & id_, Manager & manager_, Storage & storage_)
{
    atomic_data.store({id_, &manager_, &storage_});
}


IControlAttributesDriven::IControlAttributesDriven(const String & name_, Manager & manager_)
{
    auto [id, storage] = manager_.find(name_);
    atomic_data.store({id, &manager_, storage});
}


IControlAttributesDriven::IControlAttributesDriven(Manager & manager_, Storage & storage_)
{
    id =
    atomic_data.store({UUID(UInt128(0)), &manager_, std::nullopt})
}


IControlAttributesDriven::IControlAttributesDriven(const IControlAttributesDriven & src)
{
    *this = src;
}


IControlAttributesDriven & IControlAttributesDriven::operator =(const IControlAttributesDriven & src)
{
    atomic_data.store(src.atomic_data.load());
    return *this;
}


IControlAttributesDriven::~IControlAttributesDriven()
{
}


UUID IControlAttributesDriven::getID() const
{
    return UUID(loadData().id);
}


IControlAttributesStorageManager * IControlAttributesDriven::tryGetManager() const
{
    return loadData().manager;
}


IControlAttributesDriven::Data IControlAttributesDriven::loadData() const
{
    return atomic_data.load();
}


IControlAttributesDriven::Data IControlAttributesDriven::loadDataTryGetStorage() const
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
        atomic_data.compare_exchange_strong(data, new_data);
    }
    return data;
}


IControlAttributesDriven::Data IControlAttributesDriven::loadDataGetStorage() const
{
    Data data = loadDataTryGetStorage();
    if (!*data.storage)
    {
        const Type & type = getType();
        throw Exception(String(type.name) + " {" + toString(data.id) + "} not found", type.error_code_not_found);
    }
    return data;
}


IControlAttributesDriven::IDAndStorage IControlAttributesDriven::getIDAndStorage() const
{
    Data data = loadDataGetStorage();
    return {UUID(data.id), **data.storage};
}


IControlAttributesDriven::IDAndStorageAndManager IControlAttributesDriven::getIDAndStorageAndManager() const
{
    Data data = loadDataGetStorage();
    return {UUID(data.id), **data.storage, *data.manager};
}


IControlAttributesDriven::AttributesAndManager IControlAttributesDriven::getAttributesAndManager() const
{
    Data data = loadDataGetStorage();
    auto attrs = (*data.storage)->read(UUID(data.id), getType());
    return {attrs, *data.manager};
}


ControlAttributesPtr IControlAttributesDriven::getAttributes() const
{
    Data data = loadDataGetStorage();
    return (*data.storage)->read(UUID(data.id), getType());
}


ControlAttributesPtr IControlAttributesDriven::tryGetAttributes() const
{
    Data data = loadDataTryGetStorage();
    if (!*data.storage)
        return nullptr;
    return (*data.storage)->tryRead(UUID(data.id), getType());
}


void IControlAttributesDriven::insert()
{
    insertChanges().apply();
}


IControlAttributesDriven::Changes IControlAttributesDriven::insertChanges()
{
    return Changes{getIDAndStorage(), Changes::InsertTag{}, getType()};
}


void IControlAttributesDriven::setName(const String & new_name)
{
    setNameChanges(new_name).apply();
}


IControlAttributesDriven::Changes IControlAttributesDriven::setNameChanges(const String & new_name)
{
    /// Check that the new name is unique through all the storages.
    auto && [id, storage, manager] = getIDAndStorageAndManager();
    for (auto * any_storage : manager.get().getAllStorages())
    {
        auto existing_id = any_storage->find(new_name, getType());
        if (existing_id && (*existing_id != id))
        {
            auto existing_attrs = any_storage->tryRead(*existing_id);
            if (existing_attrs)
            {
                auto attrs = storage.get().tryRead(id);
                if (attrs)
                {
                    const Type & type = getType();
                    const Type & existing_type = existing_attrs->getType();
                    throw Exception(
                        String(type.name) + " " + backQuoteIfNeed(attrs->name) + ": cannot rename to " + backQuoteIfNeed(new_name)
                            + " because " + existing_type.name + " " + backQuoteIfNeed(new_name) + " already exists",
                        existing_type.error_code_already_exists);
                }
            }
        }
    }

    auto update_func = [new_name](IControlAttributes & attrs) { attrs.name = new_name; };
    return {{id, storage}, update_func, getType()};
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
    auto && [id, storage, manager] = getIDAndStorageAndManager();
    Changes changes{{id, storage}, Changes::RemoveTag{}, getType(), if_exists};

    /// We need to remove references too.
    for (auto * any_storage : manager.get().getAllStorages())
        changes.then({id, *any_storage}, Changes::RemoveReferencesTag{});
    return changes;
}


IControlAttributesDriven::Changes::Changes(const IDAndStorage & id_and_storage, InsertTag, const Type & type)
{
    then(id_and_storage, InsertTag{}, type);
}


IControlAttributesDriven::Changes::Changes(const IDAndStorage & id_and_storage, const std::function<void(IControlAttributes &)> & update_func, const Type & type, bool if_exists)
{
    then(id_and_storage, update_func, type, if_exists);
}


IControlAttributesDriven::Changes::Changes(const IDAndStorage & id_and_storage, RemoveTag, const Type & type, bool if_exists)
{
    then(id_and_storage, RemoveTag{}, type, if_exists);
}


IControlAttributesDriven::Changes::Changes(const IDAndStorage & id_and_storage, RemoveReferencesTag)
{
    then(id_and_storage, RemoveReferencesTag{});
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


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(const IDAndStorage & id_and_storage, InsertTag, const Type & type)
{
    auto & change = addChange(id_and_storage.second);
    change.change_type = Storage::ChangeType::INSERT;
    change.id = id_and_storage.first;
    change.type = &type;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(const IDAndStorage & id_and_storage, const std::function<void(IControlAttributes &)> & update_func, const Type & type, bool if_exists)
{
    auto & change = addChange(id_and_storage.second);
    change.change_type = Storage::ChangeType::UPDATE;
    change.id = id_and_storage.first;
    change.type = &type;
    change.update_func = update_func;
    change.if_exists = if_exists;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(const IDAndStorage & id_and_storage, RemoveTag, const Type & type, bool if_exists)
{
    auto & change = addChange(id_and_storage.second);
    change.change_type = Storage::ChangeType::REMOVE;
    change.id = id_and_storage.first;
    change.type = &type;
    change.if_exists = if_exists;
    return *this;
}


IControlAttributesDriven::Changes & IControlAttributesDriven::Changes::then(const IDAndStorage & id_and_storage, RemoveReferencesTag)
{
    auto & change = addChange(id_and_storage.second);
    change.change_type = Storage::ChangeType::REMOVE_REFERENCES;
    change.id = id_and_storage.first;
    return *this;
}


void IControlAttributesDriven::Changes::apply() const
{
    for (const auto & [storage, changes] : all_changes)
        storage->write(changes);
}


IControlAttributesStorage::Change & IControlAttributesDriven::Changes::addChange(Storage & storage)
{
    auto & changes = findStoragePosition(storage);
    changes.push_back({});
    return changes.back();
}


IControlAttributesStorage::Changes & IControlAttributesDriven::Changes::findStoragePosition(Storage & storage)
{
    for (auto & [stg, changes] : all_changes)
    {
        if (stg == &storage)
            return changes;
    }
    all_changes.push_back({&storage, {}});
    return all_changes.back().second;
}
}
