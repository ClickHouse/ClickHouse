#include <Access/IAccessStorage.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int ACCESS_STORAGE_READONLY;
    extern const int LOGICAL_ERROR;
}


namespace
{
    using EntityType = IAccessStorage::EntityType;
    using EntityTypeInfo = IAccessStorage::EntityTypeInfo;

    bool isNotFoundErrorCode(int error_code)
    {
        if (error_code == ErrorCodes::ACCESS_ENTITY_NOT_FOUND)
            return true;

        for (auto type : ext::range(EntityType::MAX))
            if (error_code == EntityTypeInfo::get(type).not_found_error_code)
                return true;

        return false;
    }
}


std::vector<UUID> IAccessStorage::findAll(EntityType type) const
{
    return findAllImpl(type);
}


std::optional<UUID> IAccessStorage::find(EntityType type, const String & name) const
{
    return findImpl(type, name);
}


std::vector<UUID> IAccessStorage::find(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
    {
        auto id = findImpl(type, name);
        if (id)
            ids.push_back(*id);
    }
    return ids;
}


UUID IAccessStorage::getID(EntityType type, const String & name) const
{
    auto id = findImpl(type, name);
    if (id)
        return *id;
    throwNotFound(type, name);
}


std::vector<UUID> IAccessStorage::getIDs(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
        ids.push_back(getID(type, name));
    return ids;
}


bool IAccessStorage::exists(const UUID & id) const
{
    return existsImpl(id);
}


AccessEntityPtr IAccessStorage::tryReadBase(const UUID & id) const
{
    try
    {
        return readImpl(id);
    }
    catch (Exception &)
    {
        return nullptr;
    }
}


String IAccessStorage::readName(const UUID & id) const
{
    return readNameImpl(id);
}


std::optional<String> IAccessStorage::tryReadName(const UUID & id) const
{
    try
    {
        return readNameImpl(id);
    }
    catch (Exception &)
    {
        return {};
    }
}


UUID IAccessStorage::insert(const AccessEntityPtr & entity)
{
    return insertImpl(entity, false);
}


std::vector<UUID> IAccessStorage::insert(const std::vector<AccessEntityPtr> & multiple_entities)
{
    std::vector<UUID> ids;
    ids.reserve(multiple_entities.size());
    String error_message;
    for (const auto & entity : multiple_entities)
    {
        try
        {
            ids.push_back(insertImpl(entity, false));
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS)
                throw;
            error_message += (error_message.empty() ? "" : ". ") + e.message();
        }
    }
    if (!error_message.empty())
        throw Exception(error_message, ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
    return ids;
}


std::optional<UUID> IAccessStorage::tryInsert(const AccessEntityPtr & entity)
{
    try
    {
        return insertImpl(entity, false);
    }
    catch (Exception &)
    {
        return {};
    }
}


std::vector<UUID> IAccessStorage::tryInsert(const std::vector<AccessEntityPtr> & multiple_entities)
{
    std::vector<UUID> ids;
    ids.reserve(multiple_entities.size());
    for (const auto & entity : multiple_entities)
    {
        try
        {
            ids.push_back(insertImpl(entity, false));
        }
        catch (Exception &)
        {
        }
    }
    return ids;
}


UUID IAccessStorage::insertOrReplace(const AccessEntityPtr & entity)
{
    return insertImpl(entity, true);
}


std::vector<UUID> IAccessStorage::insertOrReplace(const std::vector<AccessEntityPtr> & multiple_entities)
{
    std::vector<UUID> ids;
    ids.reserve(multiple_entities.size());
    for (const auto & entity : multiple_entities)
        ids.push_back(insertImpl(entity, true));
    return ids;
}


void IAccessStorage::remove(const UUID & id)
{
    removeImpl(id);
}


void IAccessStorage::remove(const std::vector<UUID> & ids)
{
    String error_message;
    std::optional<int> error_code;
    for (const auto & id : ids)
    {
        try
        {
            removeImpl(id);
        }
        catch (Exception & e)
        {
            if (!isNotFoundErrorCode(e.code()))
                throw;
            error_message += (error_message.empty() ? "" : ". ") + e.message();
            if (error_code && (*error_code != e.code()))
                error_code = ErrorCodes::ACCESS_ENTITY_NOT_FOUND;
            else
                error_code = e.code();
        }
    }
    if (!error_message.empty())
        throw Exception(error_message, *error_code);
}


bool IAccessStorage::tryRemove(const UUID & id)
{
    try
    {
        removeImpl(id);
        return true;
    }
    catch (Exception &)
    {
        return false;
    }
}


std::vector<UUID> IAccessStorage::tryRemove(const std::vector<UUID> & ids)
{
    std::vector<UUID> removed;
    removed.reserve(ids.size());
    for (const auto & id : ids)
    {
        try
        {
            removeImpl(id);
            removed.push_back(id);
        }
        catch (Exception &)
        {
        }
    }
    return removed;
}


void IAccessStorage::update(const UUID & id, const UpdateFunc & update_func)
{
    updateImpl(id, update_func);
}


void IAccessStorage::update(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    String error_message;
    std::optional<int> error_code;
    for (const auto & id : ids)
    {
        try
        {
            updateImpl(id, update_func);
        }
        catch (Exception & e)
        {
            if (!isNotFoundErrorCode(e.code()))
                throw;
            error_message += (error_message.empty() ? "" : ". ") + e.message();
            if (error_code && (*error_code != e.code()))
                error_code = ErrorCodes::ACCESS_ENTITY_NOT_FOUND;
            else
                error_code = e.code();
        }
    }
    if (!error_message.empty())
        throw Exception(error_message, *error_code);
}


bool IAccessStorage::tryUpdate(const UUID & id, const UpdateFunc & update_func)
{
    try
    {
        updateImpl(id, update_func);
        return true;
    }
    catch (Exception &)
    {
        return false;
    }
}


std::vector<UUID> IAccessStorage::tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    std::vector<UUID> updated;
    updated.reserve(ids.size());
    for (const auto & id : ids)
    {
        try
        {
            updateImpl(id, update_func);
            updated.push_back(id);
        }
        catch (Exception &)
        {
        }
    }
    return updated;
}


ext::scope_guard IAccessStorage::subscribeForChanges(EntityType type, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(type, handler);
}


ext::scope_guard IAccessStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(id, handler);
}


ext::scope_guard IAccessStorage::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const
{
    ext::scope_guard subscriptions;
    for (const auto & id : ids)
        subscriptions.join(subscribeForChangesImpl(id, handler));
    return subscriptions;
}


bool IAccessStorage::hasSubscription(EntityType type) const
{
    return hasSubscriptionImpl(type);
}


bool IAccessStorage::hasSubscription(const UUID & id) const
{
    return hasSubscriptionImpl(id);
}


void IAccessStorage::notify(const Notifications & notifications)
{
    for (const auto & [fn, id, new_entity] : notifications)
        fn(id, new_entity);
}


UUID IAccessStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}


Poco::Logger * IAccessStorage::getLogger() const
{
    Poco::Logger * ptr = log.load();
    if (!ptr)
        log.store(ptr = &Poco::Logger::get("Access(" + storage_name + ")"), std::memory_order_relaxed);
    return ptr;
}


void IAccessStorage::throwNotFound(const UUID & id) const
{
    throw Exception("ID {" + toString(id) + "} not found in [" + getStorageName() + "]", ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwNotFound(EntityType type, const String & name) const
{
    int error_code = EntityTypeInfo::get(type).not_found_error_code;
    throw Exception("There is no " + outputEntityTypeAndName(type, name) + " in [" + getStorageName() + "]", error_code);
}


void IAccessStorage::throwBadCast(const UUID & id, EntityType type, const String & name, EntityType required_type)
{
    throw Exception(
        "ID {" + toString(id) + "}: " + outputEntityTypeAndName(type, name) + " expected to be of type " + toString(required_type),
        ErrorCodes::LOGICAL_ERROR);
}


void IAccessStorage::throwIDCollisionCannotInsert(const UUID & id, EntityType type, const String & name, EntityType existing_type, const String & existing_name) const
{
    throw Exception(
        outputEntityTypeAndName(type, name) + ": cannot insert because the ID {" + toString(id) + "} is already used by "
            + outputEntityTypeAndName(existing_type, existing_name) + " in [" + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotInsert(EntityType type, const String & name) const
{
    throw Exception(
        outputEntityTypeAndName(type, name) + ": cannot insert because " + outputEntityTypeAndName(type, name) + " already exists in ["
            + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotRename(EntityType type, const String & old_name, const String & new_name) const
{
    throw Exception(
        outputEntityTypeAndName(type, old_name) + ": cannot rename to " + backQuote(new_name) + " because "
            + outputEntityTypeAndName(type, new_name) + " already exists in [" + getStorageName() + "]",
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwReadonlyCannotInsert(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot insert " + outputEntityTypeAndName(type, name) + " to [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotUpdate(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot update " + outputEntityTypeAndName(type, name) + " in [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotRemove(EntityType type, const String & name) const
{
    throw Exception(
        "Cannot remove " + outputEntityTypeAndName(type, name) + " from [" + getStorageName() + "] because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}
}
