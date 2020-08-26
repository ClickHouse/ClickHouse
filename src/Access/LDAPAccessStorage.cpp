#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
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


void LDAPAccessStorage::setConfiguration(AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    // TODO: switch to passing config as a ConfigurationView and remove this extra prefix once a version of Poco with proper implementation is available.
    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

    std::scoped_lock lock(mutex);

    const bool has_server = config.has(prefix_str + "server");
    const bool has_roles = config.has(prefix_str + "roles");

    if (!has_server)
        throw Exception("Missing 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    const auto ldap_server_cfg = config.getString(prefix_str + "server");
    if (ldap_server_cfg.empty())
        throw Exception("Empty 'server' field for LDAP user directory.", ErrorCodes::BAD_ARGUMENTS);

    std::set<String> roles_cfg;
    if (has_roles)
    {
        Poco::Util::AbstractConfiguration::Keys role_names;
        config.keys(prefix_str + "roles", role_names);

        // Currently, we only extract names of roles from the section names and assign them directly and unconditionally.
        roles_cfg.insert(role_names.begin(), role_names.end());
    }

    ldap_server = ldap_server_cfg;
    roles.swap(roles_cfg);
    access_control_manager = access_control_manager_;
    role_change_subscription = access_control_manager->subscribeForChanges<Role>(
        [this] (const UUID & id, const AccessEntityPtr & entity)
        {
            return this->processRoleChange(id, entity);
        }
    );
    roles_of_interest.clear();
}


bool LDAPAccessStorage::isConfiguredNoLock() const
{
    return !ldap_server.empty() &&/* !roles.empty() &&*/ access_control_manager;
}


void LDAPAccessStorage::processRoleChange(const UUID & id, const AccessEntityPtr & entity)
{
    auto role_ptr = typeid_cast<std::shared_ptr<const Role>>(entity);
    if (role_ptr)
    {
        if (roles.find(role_ptr->getName()) != roles.end())
        {
            auto update_func = [&id](const AccessEntityPtr & cached_entity) -> AccessEntityPtr
            {
                auto user_ptr = typeid_cast<std::shared_ptr<const User>>(cached_entity);
                if (user_ptr && !user_ptr->granted_roles.roles.contains(id))
                {
                    auto clone = user_ptr->clone();
                    auto user_clone_ptr = typeid_cast<std::shared_ptr<User>>(clone);
                    user_clone_ptr->granted_roles.grant(id);
                    return user_clone_ptr;
                }
                return cached_entity;
            };

            memory_storage.update(memory_storage.findAll<User>(), update_func);
            roles_of_interest.insert(id);
        }
    }
    else
    {
        if (roles_of_interest.find(id) != roles_of_interest.end())
        {
            auto update_func = [&id](const AccessEntityPtr & cached_entity) -> AccessEntityPtr
            {
                auto user_ptr = typeid_cast<std::shared_ptr<const User>>(cached_entity);
                if (user_ptr && user_ptr->granted_roles.roles.contains(id))
                {
                    auto clone = user_ptr->clone();
                    auto user_clone_ptr = typeid_cast<std::shared_ptr<User>>(clone);
                    user_clone_ptr->granted_roles.revoke(id);
                    return user_clone_ptr;
                }
                return cached_entity;
            };

            memory_storage.update(memory_storage.findAll<User>(), update_func);
            roles_of_interest.erase(id);
        }
    }
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
    return memory_storage.find(type, name);
}


std::optional<UUID> LDAPAccessStorage::findOrGenerateImpl(EntityType type, const String & name) const
{
    if (type == EntityType::USER)
    {
        std::scoped_lock lock(mutex);

        // Return the id immediately if we already have it.
        const auto id = memory_storage.find(type, name);
        if (id.has_value())
            return id;

        if (!isConfiguredNoLock())
            return {};

        // Stop if entity exists anywhere else, to avoid generating duplicates.
        const auto * this_base = dynamic_cast<const IAccessStorage *>(this);
        const auto storages = access_control_manager->getStoragesPtr();
        for (const auto & storage : *storages)
        {
            if (storage.get() != this_base && storage->find(type, name))
                return {};
        }

        // Entity doesn't exist. We are going to create one.
        const auto user = std::make_shared<User>();
        user->setName(name);
        user->authentication = Authentication(Authentication::Type::LDAP_SERVER);
        user->authentication.setServerName(ldap_server);

        for (const auto& role_name : roles)
        {
            std::optional<UUID> role_id;

            try
            {
                role_id = access_control_manager->find<Role>(role_name);
                if (!role_id)
                    throw Exception("Retrieved role info is empty", IAccessEntity::TypeInfo::get(IAccessEntity::Type::ROLE).not_found_error_code);
            }
            catch (...)
            {
                tryLogCurrentException(getLogger(), "Unable to retrieve role '" + role_name + "' info from access storage '" + access_control_manager->getStorageName() + "'");
                return {};
            }

            roles_of_interest.insert(role_id.value());
            user->granted_roles.grant(role_id.value());
        }

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
