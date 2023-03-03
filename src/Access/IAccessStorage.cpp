#include <Access/IAccessStorage.h>
#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Logger.h>
#include <base/FnTraits.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int ACCESS_STORAGE_READONLY;
    extern const int WRONG_PASSWORD;
    extern const int IP_ADDRESS_NOT_ALLOWED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int AUTHENTICATION_FAILED;
}


namespace
{
    String outputID(const UUID & id)
    {
        return "ID(" + toString(id) + ")";
    }
}


std::vector<UUID> IAccessStorage::findAll(AccessEntityType type) const
{
    return findAllImpl(type);
}


std::optional<UUID> IAccessStorage::find(AccessEntityType type, const String & name) const
{
    return findImpl(type, name);
}


std::vector<UUID> IAccessStorage::find(AccessEntityType type, const Strings & names) const
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


UUID IAccessStorage::getID(AccessEntityType type, const String & name) const
{
    auto id = findImpl(type, name);
    if (id)
        return *id;
    throwNotFound(type, name);
}


std::vector<UUID> IAccessStorage::getIDs(AccessEntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
        ids.push_back(getID(type, name));
    return ids;
}


String IAccessStorage::readName(const UUID & id) const
{
    return *readNameImpl(id, /* throw_if_not_exists = */ true);
}


std::optional<String> IAccessStorage::readName(const UUID & id, bool throw_if_not_exists) const
{
    return readNameImpl(id, throw_if_not_exists);
}


Strings IAccessStorage::readNames(const std::vector<UUID> & ids, bool throw_if_not_exists) const
{
    Strings res;
    res.reserve(ids.size());
    for (const auto & id : ids)
    {
        if (auto name = readNameImpl(id, throw_if_not_exists))
            res.emplace_back(std::move(name).value());
    }
    return res;
}


std::optional<String> IAccessStorage::tryReadName(const UUID & id) const
{
    return readName(id, /* throw_if_not_exists = */ false);
}


Strings IAccessStorage::tryReadNames(const std::vector<UUID> & ids) const
{
    return readNames(ids, /* throw_if_not_exists = */ false);
}


std::optional<String> IAccessStorage::readNameImpl(const UUID & id, bool throw_if_not_exists) const
{
    if (auto entity = read(id, throw_if_not_exists))
        return entity->getName();
    return std::nullopt;
}


UUID IAccessStorage::insert(const AccessEntityPtr & entity)
{
    return *insert(entity, /* replace_if_exists = */ false, /* throw_if_exists = */ true);
}


std::optional<UUID> IAccessStorage::insert(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists)
{
    return insertImpl(entity, replace_if_exists, throw_if_exists);
}


std::vector<UUID> IAccessStorage::insert(const std::vector<AccessEntityPtr> & multiple_entities, bool replace_if_exists, bool throw_if_exists)
{
    if (multiple_entities.empty())
        return {};

    if (multiple_entities.size() == 1)
    {
        if (auto id = insert(multiple_entities[0], replace_if_exists, throw_if_exists))
            return {*id};
        return {};
    }

    std::vector<AccessEntityPtr> successfully_inserted;
    try
    {
        std::vector<UUID> ids;
        for (const auto & entity : multiple_entities)
        {
            if (auto id = insertImpl(entity, replace_if_exists, throw_if_exists))
            {
                successfully_inserted.push_back(entity);
                ids.push_back(*id);
            }
        }
        return ids;
    }
    catch (Exception & e)
    {
        /// Try to add more information to the error message.
        if (!successfully_inserted.empty())
        {
            String successfully_inserted_str;
            for (const auto & entity : successfully_inserted)
            {
                if (!successfully_inserted_str.empty())
                    successfully_inserted_str += ", ";
                successfully_inserted_str += entity->formatTypeWithName();
            }
            e.addMessage("After successfully inserting {}/{}: {}", successfully_inserted.size(), multiple_entities.size(), successfully_inserted_str);
        }
        e.rethrow();
        __builtin_unreachable();
    }
}


std::optional<UUID> IAccessStorage::tryInsert(const AccessEntityPtr & entity)
{
    return insert(entity, /* replace_if_exists = */ false, /* throw_if_exists = */ false);
}


std::vector<UUID> IAccessStorage::tryInsert(const std::vector<AccessEntityPtr> & multiple_entities)
{
    return insert(multiple_entities, /* replace_if_exists = */ false, /* throw_if_exists = */ false);
}


UUID IAccessStorage::insertOrReplace(const AccessEntityPtr & entity)
{
    return *insert(entity, /* replace_if_exists = */ true, /* throw_if_exists = */ false);
}


std::vector<UUID> IAccessStorage::insertOrReplace(const std::vector<AccessEntityPtr> & multiple_entities)
{
    return insert(multiple_entities, /* replace_if_exists = */ true, /* throw_if_exists = */ false);
}


std::optional<UUID> IAccessStorage::insertImpl(const AccessEntityPtr & entity, bool, bool)
{
    if (isReadOnly())
        throwReadonlyCannotInsert(entity->getType(), entity->getName());
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "insertImpl() is not implemented in {}", getStorageType());
}


bool IAccessStorage::remove(const UUID & id, bool throw_if_not_exists)
{
    return removeImpl(id, throw_if_not_exists);
}


std::vector<UUID> IAccessStorage::remove(const std::vector<UUID> & ids, bool throw_if_not_exists)
{
    if (ids.empty())
        return {};
    if (ids.size() == 1)
        return remove(ids[0], throw_if_not_exists) ? ids : std::vector<UUID>{};

    Strings removed_names;
    try
    {
        std::vector<UUID> removed_ids;
        std::vector<UUID> readonly_ids;

        /// First we call remove() for non-readonly entities.
        for (const auto & id : ids)
        {
            if (isReadOnly(id))
                readonly_ids.push_back(id);
            else
            {
                auto name = tryReadName(id);
                if (remove(id, throw_if_not_exists))
                {
                    removed_ids.push_back(id);
                    if (name)
                        removed_names.push_back(std::move(name).value());
                }
            }
        }

        /// For readonly entities we're still going to call remove() because
        /// isReadOnly(id) could change and even if it's not then a storage-specific
        /// implementation of removeImpl() will probably generate a better error message.
        for (const auto & id : readonly_ids)
        {
            auto name = tryReadName(id);
            if (remove(id, throw_if_not_exists))
            {
                removed_ids.push_back(id);
                if (name)
                    removed_names.push_back(std::move(name).value());
            }
        }

        return removed_ids;
    }
    catch (Exception & e)
    {
        /// Try to add more information to the error message.
        if (!removed_names.empty())
        {
            String removed_names_str;
            for (const auto & name : removed_names)
            {
                if (!removed_names_str.empty())
                    removed_names_str += ", ";
                removed_names_str += backQuote(name);
            }
            e.addMessage("After successfully removing {}/{}: {}", removed_names.size(), ids.size(), removed_names_str);
        }
        e.rethrow();
        __builtin_unreachable();
    }
}


bool IAccessStorage::tryRemove(const UUID & id)
{
    return remove(id, /* throw_if_not_exists = */ false);
}


std::vector<UUID> IAccessStorage::tryRemove(const std::vector<UUID> & ids)
{
    return remove(ids, /* throw_if_not_exists = */ false);
}


bool IAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    if (isReadOnly(id))
    {
        auto entity = read(id, throw_if_not_exists);
        if (!entity)
            return false;
        throwReadonlyCannotRemove(entity->getType(), entity->getName());
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeImpl() is not implemented in {}", getStorageType());
}


bool IAccessStorage::update(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    return updateImpl(id, update_func, throw_if_not_exists);
}


std::vector<UUID> IAccessStorage::update(const std::vector<UUID> & ids, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    if (ids.empty())
        return {};
    if (ids.size() == 1)
        return update(ids[0], update_func, throw_if_not_exists) ? ids : std::vector<UUID>{};

    Strings names_of_updated;
    try
    {
        std::vector<UUID> ids_of_updated;
        std::vector<UUID> readonly_ids;

        /// First we call update() for non-readonly entities.
        for (const auto & id : ids)
        {
            if (isReadOnly(id))
                readonly_ids.push_back(id);
            else
            {
                auto name = tryReadName(id);
                if (update(id, update_func, throw_if_not_exists))
                {
                    ids_of_updated.push_back(id);
                    if (name)
                        names_of_updated.push_back(std::move(name).value());
                }
            }
        }

        /// For readonly entities we're still going to call update() because
        /// isReadOnly(id) could change and even if it's not then a storage-specific
        /// implementation of updateImpl() will probably generate a better error message.
        for (const auto & id : readonly_ids)
        {
            auto name = tryReadName(id);
            if (update(id, update_func, throw_if_not_exists))
            {
                ids_of_updated.push_back(id);
                if (name)
                    names_of_updated.push_back(std::move(name).value());
            }
        }

        return ids_of_updated;
    }
    catch (Exception & e)
    {
        /// Try to add more information to the error message.
        if (!names_of_updated.empty())
        {
            String names_of_updated_str;
            for (const auto & name : names_of_updated)
            {
                if (!names_of_updated_str.empty())
                    names_of_updated_str += ", ";
                names_of_updated_str += backQuote(name);
            }
            e.addMessage("After successfully updating {}/{}: {}", names_of_updated.size(), ids.size(), names_of_updated_str);
        }
        e.rethrow();
        __builtin_unreachable();
    }
}


bool IAccessStorage::tryUpdate(const UUID & id, const UpdateFunc & update_func)
{
    return update(id, update_func, /* throw_if_not_exists = */ false);
}


std::vector<UUID> IAccessStorage::tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func)
{
    return update(ids, update_func, /* throw_if_not_exists = */ false);
}


bool IAccessStorage::updateImpl(const UUID & id, const UpdateFunc &, bool throw_if_not_exists)
{
    if (isReadOnly(id))
    {
        auto entity = read(id, throw_if_not_exists);
        if (!entity)
            return false;
        throwReadonlyCannotUpdate(entity->getType(), entity->getName());
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "updateImpl() is not implemented in {}", getStorageType());
}


scope_guard IAccessStorage::subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(type, handler);
}


scope_guard IAccessStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const
{
    return subscribeForChangesImpl(id, handler);
}


scope_guard IAccessStorage::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const
{
    scope_guard subscriptions;
    for (const auto & id : ids)
        subscriptions.join(subscribeForChangesImpl(id, handler));
    return subscriptions;
}


void IAccessStorage::notify(const Notifications & notifications)
{
    for (const auto & [fn, id, new_entity] : notifications)
        fn(id, new_entity);
}


UUID IAccessStorage::authenticate(
    const Credentials & credentials,
    const Poco::Net::IPAddress & address,
    const ExternalAuthenticators & external_authenticators,
    bool allow_no_password,
    bool allow_plaintext_password) const
{
    return *authenticateImpl(credentials, address, external_authenticators, /* throw_if_user_not_exists = */ true, allow_no_password, allow_plaintext_password);
}


std::optional<UUID> IAccessStorage::authenticate(
    const Credentials & credentials,
    const Poco::Net::IPAddress & address,
    const ExternalAuthenticators & external_authenticators,
    bool throw_if_user_not_exists,
    bool allow_no_password,
    bool allow_plaintext_password) const
{
    return authenticateImpl(credentials, address, external_authenticators, throw_if_user_not_exists, allow_no_password, allow_plaintext_password);
}


std::optional<UUID> IAccessStorage::authenticateImpl(
    const Credentials & credentials,
    const Poco::Net::IPAddress & address,
    const ExternalAuthenticators & external_authenticators,
    bool throw_if_user_not_exists,
    bool allow_no_password,
    bool allow_plaintext_password) const
{
    if (auto id = find<User>(credentials.getUserName()))
    {
        if (auto user = tryRead<User>(*id))
        {
            if (!isAddressAllowed(*user, address))
                throwAddressNotAllowed(address);

            auto auth_type = user->auth_data.getType();
            if (((auth_type == AuthenticationType::NO_PASSWORD) && !allow_no_password) ||
                ((auth_type == AuthenticationType::PLAINTEXT_PASSWORD) && !allow_plaintext_password))
                throwAuthenticationTypeNotAllowed(auth_type);

            if (!areCredentialsValid(*user, credentials, external_authenticators))
                throwInvalidCredentials();

            return id;
        }
    }

    if (throw_if_user_not_exists)
        throwNotFound(AccessEntityType::USER, credentials.getUserName());
    else
        return std::nullopt;
}


bool IAccessStorage::areCredentialsValid(
    const User & user,
    const Credentials & credentials,
    const ExternalAuthenticators & external_authenticators) const
{
    if (!credentials.isReady())
        return false;

    if (credentials.getUserName() != user.getName())
        return false;

    return Authentication::areCredentialsValid(credentials, user.auth_data, external_authenticators);
}


bool IAccessStorage::isAddressAllowed(const User & user, const Poco::Net::IPAddress & address) const
{
    return user.allowed_client_hosts.contains(address);
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
    throw Exception(outputID(id) + " not found in " + getStorageName(), ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwNotFound(AccessEntityType type, const String & name) const
{
    int error_code = AccessEntityTypeInfo::get(type).not_found_error_code;
    throw Exception("There is no " + formatEntityTypeWithName(type, name) + " in " + getStorageName(), error_code);
}


void IAccessStorage::throwBadCast(const UUID & id, AccessEntityType type, const String & name, AccessEntityType required_type)
{
    throw Exception(
        outputID(id) + ": " + formatEntityTypeWithName(type, name) + " expected to be of type " + toString(required_type),
        ErrorCodes::LOGICAL_ERROR);
}


void IAccessStorage::throwIDCollisionCannotInsert(const UUID & id, AccessEntityType type, const String & name, AccessEntityType existing_type, const String & existing_name) const
{
    throw Exception(
        formatEntityTypeWithName(type, name) + ": cannot insert because the " + outputID(id) + " is already used by "
            + formatEntityTypeWithName(existing_type, existing_name) + " in " + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotInsert(AccessEntityType type, const String & name) const
{
    throw Exception(
        formatEntityTypeWithName(type, name) + ": cannot insert because " + formatEntityTypeWithName(type, name) + " already exists in "
            + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwNameCollisionCannotRename(AccessEntityType type, const String & old_name, const String & new_name) const
{
    throw Exception(
        formatEntityTypeWithName(type, old_name) + ": cannot rename to " + backQuote(new_name) + " because "
            + formatEntityTypeWithName(type, new_name) + " already exists in " + getStorageName(),
        ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
}


void IAccessStorage::throwReadonlyCannotInsert(AccessEntityType type, const String & name) const
{
    throw Exception(
        "Cannot insert " + formatEntityTypeWithName(type, name) + " to " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotUpdate(AccessEntityType type, const String & name) const
{
    throw Exception(
        "Cannot update " + formatEntityTypeWithName(type, name) + " in " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}


void IAccessStorage::throwReadonlyCannotRemove(AccessEntityType type, const String & name) const
{
    throw Exception(
        "Cannot remove " + formatEntityTypeWithName(type, name) + " from " + getStorageName() + " because this storage is readonly",
        ErrorCodes::ACCESS_STORAGE_READONLY);
}

void IAccessStorage::throwAddressNotAllowed(const Poco::Net::IPAddress & address)
{
    throw Exception("Connections from " + address.toString() + " are not allowed", ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
}

void IAccessStorage::throwAuthenticationTypeNotAllowed(AuthenticationType auth_type)
{
    throw Exception(
        ErrorCodes::AUTHENTICATION_FAILED,
        "Authentication type {} is not allowed, check the setting allow_{} in the server configuration",
        toString(auth_type), AuthenticationTypeInfo::get(auth_type).name);
}
void IAccessStorage::throwInvalidCredentials()
{
    throw Exception("Invalid credentials", ErrorCodes::WRONG_PASSWORD);
}

}
