#include <Access/IAccessStorage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_CAST;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_ENTITY_FOUND_DUPLICATES;
    extern const int ACCESS_ENTITY_STORAGE_READONLY;
}


std::vector<UUID> IAccessStorage::findAll(std::type_index type) const
{
    return findAllImpl(type);
}


std::optional<UUID> IAccessStorage::find(std::type_index type, const String & name) const
{
    return findImpl(type, name);
}


std::vector<UUID> IAccessStorage::find(std::type_index type, const Strings & names) const
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


UUID IAccessStorage::getID(std::type_index type, const String & name) const
{
    auto id = findImpl(type, name);
    if (id)
        return *id;
    throwNotFound(type, name);
}


std::vector<UUID> IAccessStorage::getIDs(std::type_index type, const Strings & names) const
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
    for (const auto & id : ids)
    {
        try
        {
            removeImpl(id);
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::ACCESS_ENTITY_NOT_FOUND)
                throw;
            error_message += (error_message.empty() ? "" : ". ") + e.message();
        }
    }
    if (!error_message.empty())
        throw Exception(error_message, ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
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
    for (const auto & id : ids)
    {
        try
        {
            updateImpl(id, update_func);
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::ACCESS_ENTITY_NOT_FOUND)
                throw;
            error_message += (error_message.empty() ? "" : ". ") + e.message();
        }
    }
    if (!error_message.empty())
        throw Exception(error_message, ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
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


IAccessStorage::SubscriptionPtr IAccessStorage::subscribeForChanges(std::type_index type, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(type, handler);
}


IAccessStorage::SubscriptionPtr IAccessStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(id, handler);
}


IAccessStorage::SubscriptionPtr IAccessStorage::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const
{
    if (ids.empty())
        return nullptr;
    if (ids.size() == 1)
        return subscribeForChangesImpl(ids[0], handler);

    std::vector<SubscriptionPtr> subscriptions;
    subscriptions.reserve(ids.size());
    for (const auto & id : ids)
    {
        auto subscription = subscribeForChangesImpl(id, handler);
        if (subscription)
            subscriptions.push_back(std::move(subscription));
    }

    class SubscriptionImpl : public Subscription
    {
    public:
        SubscriptionImpl(std::vector<SubscriptionPtr> subscriptions_)
            : subscriptions(std::move(subscriptions_)) {}
    private:
        std::vector<SubscriptionPtr> subscriptions;
    };

    return std::make_unique<SubscriptionImpl>(std::move(subscriptions));
}


bool IAccessStorage::hasSubscription(std::type_index type) const
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
    throw Exception("ID {" + toString(id) + "} not found in " + getStorageName(), ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwNotFound(std::type_index type, const String & name) const
{
    throw Exception(
        getTypeName(type) + " " + backQuote(name) + " not found in " + getStorageName(), ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwBadCast(const UUID & id, std::type_index type, const String & name, std::type_index required_type) const
{
    throw Exception(
        "ID {" + toString(id) + "}: " + getTypeName(type) + backQuote(name) + " expected to be of type " + getTypeName(required_type),
        ErrorCodes::BAD_CAST);
}


void IAccessStorage::throwIDCollisionCannotInsert(const UUID & id, std::type_index type, const String & name, std::type_index existing_type, const String & existing_name) const
{
    throw Exception(
        getTypeName(type) + " " + backQuote(name) + ": cannot insert because the ID {" + toString(id) + "} is already used by "
            + getTypeName(existing_type) + " " + backQuote(existing_name) + " in " + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotInsert(std::type_index type, const String & name) const
{
    throw Exception(
        getTypeName(type) + " " + backQuote(name) + ": cannot insert because " + getTypeName(type) + " " + backQuote(name)
            + " already exists in " + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotRename(std::type_index type, const String & old_name, const String & new_name) const
{
    throw Exception(
        getTypeName(type) + " " + backQuote(old_name) + ": cannot rename to " + backQuote(new_name) + " because " + getTypeName(type) + " "
            + backQuote(new_name) + " already exists in " + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwReadonlyCannotInsert(std::type_index type, const String & name) const
{
    throw Exception(
        "Cannot insert " + getTypeName(type) + " " + backQuote(name) + " to " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_ENTITY_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotUpdate(std::type_index type, const String & name) const
{
    throw Exception(
        "Cannot update " + getTypeName(type) + " " + backQuote(name) + " in " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_ENTITY_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotRemove(std::type_index type, const String & name) const
{
    throw Exception(
        "Cannot remove " + getTypeName(type) + " " + backQuote(name) + " from " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_ENTITY_STORAGE_READONLY);
}
}
