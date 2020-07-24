#include <Access/LDAPAccessStorage.h>
#include <Access/User.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


LDAPAccessStorage::LDAPAccessStorage() : IAccessStorage("ldap")
{
}


void LDAPAccessStorage::setConfiguration(const Poco::Util::AbstractConfiguration & config, IAccessStorage * top_enclosing_storage_)
{
    std::scoped_lock lock(mutex);

    const bool has_server = config.has("server");
    const bool has_user_template = config.has("user_template");

    if (!has_server)
        throw Exception("Missing 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    const auto ldap_server_cfg = config.getString("server");
    const auto user_template_cfg = (has_user_template ? config.getString("user_template") : "default");

    if (ldap_server_cfg.empty())
        throw Exception("Empty 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    if (user_template_cfg.empty())
        throw Exception("Empty 'user_template' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    ldap_server = ldap_server_cfg;
    user_template = user_template_cfg;
    top_enclosing_storage = top_enclosing_storage_;
}


bool LDAPAccessStorage::isConfiguredNoLock() const
{
    return !ldap_server.empty() && !user_template.empty() && top_enclosing_storage;
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
        const auto user_tmp = top_enclosing_storage->read<User>(user_template);
        if (!user_tmp)
        {
            LOG_WARNING(getLogger(), "Unable to retrieve user template '{}': user does not exist in access storage '{}'.", user_template, top_enclosing_storage->getStorageName());
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
