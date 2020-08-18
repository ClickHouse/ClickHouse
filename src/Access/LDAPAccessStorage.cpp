#include <Access/LDAPAccessStorage.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


LDAPAccessStorage::LDAPAccessStorage(const String & storage_name_)
    : IAccessStorage(storage_name_)
{
}


void LDAPAccessStorage::setConfiguration(IAccessStorage * top_enclosing_storage_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

    std::scoped_lock lock(mutex);

    const bool has_server = config.has(prefix_str + "server");
    const bool has_user_template = config.has(prefix_str + "user_template");

    if (!has_server)
        throw Exception("Missing 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    const auto ldap_server_cfg = config.getString(prefix_str + "server");
    String user_template_cfg;

    if (ldap_server_cfg.empty())
        throw Exception("Empty 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    if (has_user_template)
        user_template_cfg = config.getString(prefix_str + "user_template");

    if (user_template_cfg.empty())
        user_template_cfg = "default";

    ldap_server = ldap_server_cfg;
    user_template = user_template_cfg;
    top_enclosing_storage = top_enclosing_storage_;
}


bool LDAPAccessStorage::isConfiguredNoLock() const
{
    return !ldap_server.empty() && !user_template.empty() && top_enclosing_storage;
}


const char * LDAPAccessStorage::getStorageType() const
{
    return STORAGE_TYPE;
}


bool LDAPAccessStorage::isStorageReadOnly() const
{
    return true;
}


std::optional<UUID> LDAPAccessStorage::findImpl(EntityType type, const String & name) const
{
    if (type == EntityType::USER)
    {
        std::scoped_lock lock(mutex);

        // Detect and avoid loops/duplicate creations.
        if (helper_lookup_in_progress)
            return {};

        helper_lookup_in_progress = true;
        SCOPE_EXIT({ helper_lookup_in_progress = false; });

        // Return the id immediately if we already have it.
        const auto id = memory_storage.find(type, name);
        if (id.has_value())
            return id;

        if (!isConfiguredNoLock())
            return {};

        // Stop if entity exists anywhere else, to avoid duplicates.
        if (top_enclosing_storage->find(type, name))
            return {};

        // Entity doesn't exist. We are going to create one.

        // Retrieve the template first.
        std::shared_ptr<const User> user_tmp;
        try
        {
            user_tmp = top_enclosing_storage->read<User>(user_template);
            if (!user_tmp)
                throw Exception("Retrieved user is empty", IAccessEntity::TypeInfo::get(IAccessEntity::Type::USER).not_found_error_code);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Unable to retrieve user template '" + user_template + "' from access storage '" + top_enclosing_storage->getStorageName() + "'");
            return {};
        }

        // Build the new entity based on the existing template.
        const auto user = std::make_shared<User>(*user_tmp);
        user->setName(name);
        user->authentication = Authentication(Authentication::Type::LDAP_SERVER);
        user->authentication.setServerName(ldap_server);

        return memory_storage.insert(user);
    }

    return memory_storage.find(type, name);
}


std::vector<UUID> LDAPAccessStorage::findAllImpl(EntityType type) const
{
    return memory_storage.findAll(type);
}


bool LDAPAccessStorage::existsImpl(const UUID & id) const
{
    return memory_storage.exists(id);
}


AccessEntityPtr LDAPAccessStorage::readImpl(const UUID & id) const
{
    return memory_storage.read(id);
}


String LDAPAccessStorage::readNameImpl(const UUID & id) const
{
    return memory_storage.readName(id);
}


bool LDAPAccessStorage::canInsertImpl(const AccessEntityPtr &) const
{
    return false;
}


UUID LDAPAccessStorage::insertImpl(const AccessEntityPtr & entity, bool)
{
    throwReadonlyCannotInsert(entity->getType(), entity->getName());
}


void LDAPAccessStorage::removeImpl(const UUID & id)
{
    auto entity = read(id);
    throwReadonlyCannotRemove(entity->getType(), entity->getName());
}


void LDAPAccessStorage::updateImpl(const UUID & id, const UpdateFunc &)
{
    auto entity = read(id);
    throwReadonlyCannotUpdate(entity->getType(), entity->getName());
}


ext::scope_guard LDAPAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(id, handler);
}


ext::scope_guard LDAPAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(type, handler);
}


bool LDAPAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    return memory_storage.hasSubscription(id);
}


bool LDAPAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    return memory_storage.hasSubscription(type);
}
}
