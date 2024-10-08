#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Credentials.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/container_hash/hash.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <iterator>
#include <sstream>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


LDAPAccessStorage::LDAPAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
    : IAccessStorage(storage_name_), access_control(access_control_), memory_storage(storage_name_, access_control.getChangesNotifier(), false)
{
    setConfiguration(config, prefix);
}


String LDAPAccessStorage::getLDAPServerName() const
{
    std::lock_guard lock(mutex);
    return ldap_server_name;
}


void LDAPAccessStorage::setConfiguration(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    std::lock_guard lock(mutex);

    // TODO: switch to passing config as a ConfigurationView and remove this extra prefix once a version of Poco with proper implementation is available.
    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

    const bool has_server = config.has(prefix_str + "server");
    const bool has_roles = config.has(prefix_str + "roles");
    const bool has_role_mapping = config.has(prefix_str + "role_mapping");

    if (!has_server)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'server' field for LDAP user directory");

    const auto ldap_server_name_cfg = config.getString(prefix_str + "server");
    if (ldap_server_name_cfg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'server' field for LDAP user directory");

    std::set<String> common_roles_cfg;
    if (has_roles)
    {
        Poco::Util::AbstractConfiguration::Keys role_names;
        config.keys(prefix_str + "roles", role_names);

        // Currently, we only extract names of roles from the section names and assign them directly and unconditionally.
        common_roles_cfg.insert(role_names.begin(), role_names.end());
    }

    LDAPClient::RoleSearchParamsList role_search_params_cfg;
    if (has_role_mapping)
    {
        Poco::Util::AbstractConfiguration::Keys all_keys;
        config.keys(prefix, all_keys);
        for (const auto & key : all_keys)
        {
            if (key == "role_mapping" || key.starts_with("role_mapping["))
                parseLDAPRoleSearchParams(role_search_params_cfg.emplace_back(), config, prefix_str + key);
        }
    }

    ldap_server_name = ldap_server_name_cfg;
    role_search_params.swap(role_search_params_cfg);
    common_role_names.swap(common_roles_cfg);

    external_role_hashes.clear();
    users_per_roles.clear();
    roles_per_users.clear();
    granted_role_names.clear();
    granted_role_ids.clear();

    role_change_subscription = access_control.subscribeForChanges<Role>(
        [this] (const UUID & id, const AccessEntityPtr & entity)
        {
            this->processRoleChange(id, entity);
        }
    );
}


void LDAPAccessStorage::processRoleChange(const UUID & id, const AccessEntityPtr & entity)
{
    std::lock_guard lock(mutex);
    const auto role = typeid_cast<std::shared_ptr<const Role>>(entity);
    const auto it = granted_role_names.find(id);

    if (role) // Added or renamed a role.
    {
        const auto & new_role_name = role->getName();
        if (it != granted_role_names.end()) // Renamed a granted role.
        {
            const auto & old_role_name = it->second;
            if (new_role_name != old_role_name)
            {
                // Revoke the old role first, then grant the new role.
                applyRoleChangeNoLock(false /* revoke */, id, old_role_name);
                applyRoleChangeNoLock(true /* grant */, id, new_role_name);
            }
        }
        else // Added a role.
        {
            applyRoleChangeNoLock(true /* grant */, id, new_role_name);
        }
    }
    else // Removed a role.
    {
        if (it != granted_role_names.end()) // Removed a granted role.
        {
            const auto & old_role_name = it->second;
            applyRoleChangeNoLock(false /* revoke */, id, old_role_name);
        }
    }
}


void LDAPAccessStorage::applyRoleChangeNoLock(bool grant, const UUID & role_id, const String & role_name)
{
    std::vector<UUID> user_ids;

    // Build a list of ids of the relevant users.
    if (common_role_names.contains(role_name))
    {
        user_ids = memory_storage.findAll<User>();
    }
    else
    {
        const auto it = users_per_roles.find(role_name);
        if (it != users_per_roles.end())
        {
            const auto & user_names = it->second;
            user_ids.reserve(user_names.size());

            for (const auto & user_name : user_names)
            {
                if (const auto user_id = memory_storage.find<User>(user_name))
                    user_ids.emplace_back(*user_id);
            }
        }
    }

    // Update the granted roles of the relevant users.
    if (!user_ids.empty())
    {
        auto update_func = [&role_id, &grant] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
        {
            if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
            {
                auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
                if (grant)
                    changed_user->granted_roles.grant(role_id);
                else
                    changed_user->granted_roles.revoke(role_id);
                return changed_user;
            }
            return entity_;
        };

        memory_storage.update(user_ids, update_func);
    }

    // Actualize granted_role_* mappings.
    if (grant)
    {
        if (!user_ids.empty())
        {
            granted_role_names.insert_or_assign(role_id, role_name);
            granted_role_ids.insert_or_assign(role_name, role_id);
        }
    }
    else
    {
        granted_role_ids.erase(role_name);
        granted_role_names.erase(role_id);
    }
}


void LDAPAccessStorage::assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles) const
{
    const auto external_roles_hash = boost::hash<LDAPClient::SearchResultsList>{}(external_roles);
    assignRolesNoLock(user, external_roles, external_roles_hash);
}


void LDAPAccessStorage::assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles, std::size_t external_roles_hash) const
{
    const auto & user_name = user.getName();
    auto & granted_roles = user.granted_roles;
    auto local_role_names = mapExternalRolesNoLock(external_roles);

    auto grant_role = [this, &user_name, &granted_roles] (const String & role_name, const bool common)
    {
        auto it = granted_role_ids.find(role_name);
        if (it == granted_role_ids.end())
        {
            if (const auto role_id = access_control.find<Role>(role_name))
            {
                granted_role_names.insert_or_assign(*role_id, role_name);
                it = granted_role_ids.insert_or_assign(role_name, *role_id).first;
            }
        }

        if (it != granted_role_ids.end())
        {
            const auto & role_id = it->second;
            granted_roles.grant(role_id);
        }
        else
        {
            LOG_WARNING(getLogger(), "Unable to grant {} role '{}' to user '{}': role not found", (common ? "common" : "mapped"), role_name, user_name);
        }
    };

    external_role_hashes.erase(user_name);
    granted_roles = {};
    const auto old_role_names = std::move(roles_per_users[user_name]);

    // Grant the common roles first.
    for (const auto & role_name : common_role_names)
    {
        grant_role(role_name, true /* common */);
    }

    // Grant the mapped external roles and actualize users_per_roles mapping.
    // local_role_names allowed to overlap with common_role_names.
    for (const auto & role_name : local_role_names)
    {
        grant_role(role_name, false /* mapped */);
        users_per_roles[role_name].insert(user_name);
    }

    // Cleanup users_per_roles and granted_role_* mappings.
    for (const auto & old_role_name : old_role_names)
    {
        if (local_role_names.contains(old_role_name))
            continue;

        const auto rit = users_per_roles.find(old_role_name);
        if (rit == users_per_roles.end())
            continue;

        auto & user_names = rit->second;
        user_names.erase(user_name);

        if (!user_names.empty())
            continue;

        users_per_roles.erase(rit);

        if (common_role_names.contains(old_role_name))
            continue;

        const auto iit = granted_role_ids.find(old_role_name);
        if (iit == granted_role_ids.end())
            continue;

        const auto old_role_id = iit->second;
        granted_role_names.erase(old_role_id);
        granted_role_ids.erase(iit);
    }

    // Actualize roles_per_users mapping and external_role_hashes cache.
    if (local_role_names.empty())
        roles_per_users.erase(user_name);
    else
        roles_per_users[user_name] = std::move(local_role_names);

    external_role_hashes[user_name] = external_roles_hash;
}


void LDAPAccessStorage::updateAssignedRolesNoLock(const UUID & id, const String & user_name, const LDAPClient::SearchResultsList & external_roles) const
{
    // No need to include common_role_names in this hash each time, since they don't change.
    const auto external_roles_hash = boost::hash<LDAPClient::SearchResultsList>{}(external_roles);

    // Map and grant the roles from scratch only if the list of external role has changed.
    const auto it = external_role_hashes.find(user_name);
    if (it != external_role_hashes.end() && it->second == external_roles_hash)
        return;

    auto update_func = [this, &external_roles, external_roles_hash] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
    {
        if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
        {
            auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
            assignRolesNoLock(*changed_user, external_roles, external_roles_hash);
            return changed_user;
        }
        return entity_;
    };

    memory_storage.update(id, update_func);
}


std::set<String> LDAPAccessStorage::mapExternalRolesNoLock(const LDAPClient::SearchResultsList & external_roles) const
{
    std::set<String> role_names;

    if (external_roles.size() != role_search_params.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to map external roles");

    for (std::size_t i = 0; i < external_roles.size(); ++i)
    {
        const auto & external_role_set = external_roles[i];
        const auto & prefix = role_search_params[i].prefix;

        for (const auto & external_role : external_role_set)
        {
            if (prefix.size() < external_role.size() && external_role.starts_with(prefix))
            {
                role_names.emplace(external_role, prefix.size());
            }
        }
    }

    return role_names;
}


bool LDAPAccessStorage::areLDAPCredentialsValidNoLock(const User & user, const Credentials & credentials,
    const ExternalAuthenticators & external_authenticators, LDAPClient::SearchResultsList & role_search_results) const
{
    if (!credentials.isReady())
        return false;

    if (credentials.getUserName() != user.getName())
        return false;

    if (typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    if (const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials))
        return external_authenticators.checkLDAPCredentials(ldap_server_name, *basic_credentials, &role_search_params, &role_search_results);

    return false;
}


const char * LDAPAccessStorage::getStorageType() const
{
    return STORAGE_TYPE;
}


String LDAPAccessStorage::getStorageParamsJSON() const
{
    std::lock_guard lock(mutex);
    Poco::JSON::Object params_json;

    params_json.set("server", ldap_server_name);

    Poco::JSON::Array common_role_names_json;
    for (const auto & role : common_role_names)
    {
        common_role_names_json.add(role);
    }
    params_json.set("roles", common_role_names_json);

    Poco::JSON::Array role_mappings_json;
    for (const auto & role_mapping : role_search_params)
    {
        Poco::JSON::Object role_mapping_json;

        role_mapping_json.set("base_dn", role_mapping.base_dn);
        role_mapping_json.set("search_filter", role_mapping.search_filter);
        role_mapping_json.set("attribute", role_mapping.attribute);
        role_mapping_json.set("prefix", role_mapping.prefix);

        String scope;
        switch (role_mapping.scope)
        {
            case LDAPClient::SearchParams::Scope::BASE:      scope = "base"; break;
            case LDAPClient::SearchParams::Scope::ONE_LEVEL: scope = "one_level"; break;
            case LDAPClient::SearchParams::Scope::SUBTREE:   scope = "subtree"; break;
            case LDAPClient::SearchParams::Scope::CHILDREN:  scope = "children"; break;
        }
        role_mapping_json.set("scope", scope);

        role_mappings_json.add(role_mapping_json);
    }
    params_json.set("role_mappings", role_mappings_json);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(params_json, oss);

    return oss.str();
}


std::optional<UUID> LDAPAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock(mutex);
    return memory_storage.find(type, name);
}


std::vector<UUID> LDAPAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock(mutex);
    return memory_storage.findAll(type);
}


bool LDAPAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock(mutex);
    return memory_storage.exists(id);
}


AccessEntityPtr LDAPAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock(mutex);
    return memory_storage.read(id, throw_if_not_exists);
}


std::optional<std::pair<String, AccessEntityType>> LDAPAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock(mutex);
    return memory_storage.readNameWithType(id, throw_if_not_exists);
}


std::optional<AuthResult> LDAPAccessStorage::authenticateImpl(
    const Credentials & credentials,
    const Poco::Net::IPAddress & address,
    const ExternalAuthenticators & external_authenticators,
    bool throw_if_user_not_exists,
    bool /* allow_no_password */,
    bool /* allow_plaintext_password */) const
{
    std::lock_guard lock(mutex);
    auto id = memory_storage.find<User>(credentials.getUserName());
    UserPtr user = id ? memory_storage.read<User>(*id) : nullptr;

    std::shared_ptr<User> new_user;
    if (!user)
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        new_user = std::make_shared<User>();
        new_user->setName(credentials.getUserName());
        new_user->authentication_methods.emplace_back(AuthenticationType::LDAP);
        new_user->authentication_methods.back().setLDAPServerName(ldap_server_name);
        user = new_user;
    }

    if (!isAddressAllowed(*user, address))
        throwAddressNotAllowed(address);

    LDAPClient::SearchResultsList external_roles;
    if (!areLDAPCredentialsValidNoLock(*user, credentials, external_authenticators, external_roles))
    {
        // We don't know why the authentication has just failed:
        // either there is no such user in LDAP or the password is not correct.
        // We treat this situation as if there is no such user because we don't want to block
        // other storages following this LDAPAccessStorage from trying to authenticate on their own.
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, credentials.getUserName());
        else
            return {};
    }

    if (new_user)
    {
        // TODO: if these were AlwaysAllowCredentials, then mapped external roles are not available here,
        // since without a password we can't authenticate and retrieve roles from the LDAP server.

        assignRolesNoLock(*new_user, external_roles);
        id = memory_storage.insert(new_user);
    }
    else
    {
        // Just in case external_roles are changed. This will be no-op if they are not.
        updateAssignedRolesNoLock(*id, user->getName(), external_roles);
    }

    if (id)
        return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::LDAP) };
    return std::nullopt;
}

}
