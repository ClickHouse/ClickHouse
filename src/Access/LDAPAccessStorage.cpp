#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/range/algorithm/copy.hpp>
#include <iterator>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


LDAPAccessStorage::LDAPAccessStorage(const String & storage_name_, AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
    : IAccessStorage(storage_name_)
{
    setConfiguration(access_control_manager_, config, prefix);
}


String LDAPAccessStorage::getLDAPServerName() const
{
    return ldap_server;
}


void LDAPAccessStorage::setConfiguration(AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    std::scoped_lock lock(mutex);

    // TODO: switch to passing config as a ConfigurationView and remove this extra prefix once a version of Poco with proper implementation is available.
    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

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

    access_control_manager = access_control_manager_;
    ldap_server = ldap_server_cfg;
    default_role_names.swap(roles_cfg);
    roles_of_interest.clear();
    role_change_subscription = access_control_manager->subscribeForChanges<Role>(
        [this] (const UUID & id, const AccessEntityPtr & entity)
        {
            return this->processRoleChange(id, entity);
        }
    );

    /// Update `roles_of_interests` with initial values.
    for (const auto & role_name : default_role_names)
    {
        if (auto role_id = access_control_manager->find<Role>(role_name))
            roles_of_interest.emplace(*role_id, role_name);
    }
}


void LDAPAccessStorage::processRoleChange(const UUID & id, const AccessEntityPtr & entity)
{
    std::scoped_lock lock(mutex);

    /// Update `roles_of_interests`.
    auto role = typeid_cast<std::shared_ptr<const Role>>(entity);
    bool need_to_update_users = false;

    if (role && default_role_names.count(role->getName()))
    {
        /// If a role was created with one of the `default_role_names` or renamed to one of the `default_role_names`,
        /// then set `need_to_update_users`.
        need_to_update_users = roles_of_interest.insert_or_assign(id, role->getName()).second;
    }
    else
    {
        /// If a role was removed or renamed to a name which isn't contained in the `default_role_names`,
        /// then set `need_to_update_users`.
        need_to_update_users = roles_of_interest.erase(id) > 0;
    }

    /// Update users which have been created.
    if (need_to_update_users)
    {
        auto update_func = [this] (const AccessEntityPtr & entity_) -> AccessEntityPtr
        {
            if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
            {
                auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
                auto & granted_roles = changed_user->granted_roles.roles;
                granted_roles.clear();
                boost::range::copy(roles_of_interest | boost::adaptors::map_keys, std::inserter(granted_roles, granted_roles.end()));
                return changed_user;
            }
            return entity_;
        };
        memory_storage.update(memory_storage.findAll<User>(), update_func);
    }
}


void LDAPAccessStorage::checkAllDefaultRoleNamesFoundNoLock() const
{
    boost::container::flat_set<std::string_view> role_names_of_interest;
    boost::range::copy(roles_of_interest | boost::adaptors::map_values, std::inserter(role_names_of_interest, role_names_of_interest.end()));

    for (const auto & role_name : default_role_names)
    {
        if (!role_names_of_interest.count(role_name))
            throwDefaultRoleNotFound(role_name);
    }
}


const char * LDAPAccessStorage::getStorageType() const
{
    return STORAGE_TYPE;
}


String LDAPAccessStorage::getStorageParamsJSON() const
{
    std::scoped_lock lock(mutex);
    Poco::JSON::Object params_json;

    params_json.set("server", ldap_server);
    params_json.set("roles", default_role_names);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(params_json, oss);

    return oss.str();
}


std::optional<UUID> LDAPAccessStorage::findImpl(EntityType type, const String & name) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.find(type, name);
}


std::vector<UUID> LDAPAccessStorage::findAllImpl(EntityType type) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.findAll(type);
}


bool LDAPAccessStorage::existsImpl(const UUID & id) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.exists(id);
}


AccessEntityPtr LDAPAccessStorage::readImpl(const UUID & id) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.read(id);
}


String LDAPAccessStorage::readNameImpl(const UUID & id) const
{
    std::scoped_lock lock(mutex);
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
    std::scoped_lock lock(mutex);
    auto entity = read(id);
    throwReadonlyCannotRemove(entity->getType(), entity->getName());
}


void LDAPAccessStorage::updateImpl(const UUID & id, const UpdateFunc &)
{
    std::scoped_lock lock(mutex);
    auto entity = read(id);
    throwReadonlyCannotUpdate(entity->getType(), entity->getName());
}


ext::scope_guard LDAPAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.subscribeForChanges(id, handler);
}


ext::scope_guard LDAPAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.subscribeForChanges(type, handler);
}


bool LDAPAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.hasSubscription(id);
}


bool LDAPAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::scoped_lock lock(mutex);
    return memory_storage.hasSubscription(type);
}

UUID LDAPAccessStorage::loginImpl(const String & user_name, const String & password, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators) const
{
    std::scoped_lock lock(mutex);
    auto id = memory_storage.find<User>(user_name);
    if (id)
    {
        auto user = memory_storage.read<User>(*id);

        if (!isPasswordCorrectImpl(*user, password, external_authenticators))
            throwInvalidPassword();

        if (!isAddressAllowedImpl(*user, address))
            throwAddressNotAllowed(address);

        return *id;
    }
    else
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        auto user = std::make_shared<User>();
        user->setName(user_name);
        user->authentication = Authentication(Authentication::Type::LDAP_SERVER);
        user->authentication.setServerName(ldap_server);

        if (!isPasswordCorrectImpl(*user, password, external_authenticators))
            throwInvalidPassword();

        if (!isAddressAllowedImpl(*user, address))
            throwAddressNotAllowed(address);

        checkAllDefaultRoleNamesFoundNoLock();

        auto & granted_roles = user->granted_roles.roles;
        boost::range::copy(roles_of_interest | boost::adaptors::map_keys, std::inserter(granted_roles, granted_roles.end()));

        return memory_storage.insert(user);
    }
}

UUID LDAPAccessStorage::getIDOfLoggedUserImpl(const String & user_name) const
{
    std::scoped_lock lock(mutex);
    auto id = memory_storage.find<User>(user_name);
    if (id)
    {
        return *id;
    }
    else
    {
        // User does not exist, so we create one, and add it pretending that the authentication is successful.
        auto user = std::make_shared<User>();
        user->setName(user_name);
        user->authentication = Authentication(Authentication::Type::LDAP_SERVER);
        user->authentication.setServerName(ldap_server);

        checkAllDefaultRoleNamesFoundNoLock();

        auto & granted_roles = user->granted_roles.roles;
        boost::range::copy(roles_of_interest | boost::adaptors::map_keys, std::inserter(granted_roles, granted_roles.end()));

        return memory_storage.insert(user);
    }
}

void LDAPAccessStorage::throwDefaultRoleNotFound(const String & role_name)
{
    throw Exception("One of the default roles, the role '" + role_name + "', is not found", IAccessEntity::TypeInfo::get(IAccessEntity::Type::ROLE).not_found_error_code);
}

}
