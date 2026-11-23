#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Credentials.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char ldap_access_storage_fail_role_reconciliation[];
    extern const char ldap_access_storage_fail_role_reconciliation_after_first_user[];
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
        // A dot in a section name is escaped with a backslash by `Poco::Util::XMLConfiguration`, so remove the backslash
        // to get the real name of the role (roles defined in `users.xml` are also stored under the un-escaped names).
        for (const auto & role_name : role_names)
            common_roles_cfg.insert(Poco::replace(role_name, "\\.", "."));
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

    users_external_roles.clear();
    users_per_roles.clear();
    roles_per_users.clear();
    granted_role_ids.clear();
    pending_role_ids.clear();
    pending_role_may_exist = false;
    unreconciled_role_ids.clear();

    role_change_subscription = access_control.subscribeForChanges<Role>(
        [this] (const std::vector<AccessChangesNotifier::Change> & changes)
        {
            this->processRoleChanges(changes);
        }
    );
}


void LDAPAccessStorage::processRoleChanges(const std::vector<AccessChangesNotifier::Change> & changes)
{
    std::lock_guard lock(mutex);

    // The changes are accumulated in `pending_role_ids` and are removed from there only after every affected user is
    // updated. If the reconciliation fails (e.g. because of a memory limit), the changes stay pending along with
    // the previous resolution of the names in `granted_role_ids`, and they are reconciled again on the next call:
    // `AccessChangesNotifier` calls this handler on every `sendNotifications`, with an empty list if no role has changed.
    for (const auto & change : changes)
    {
        pending_role_ids.insert(change.id);
        if (typeid_cast<std::shared_ptr<const Role>>(change.entity))
            pending_role_may_exist = true;
    }

    if (pending_role_ids.empty())
        return;

    // The whole batch of changes is reconciled at once, so that a reload changing many roles resolves every configured
    // or mapped name only once instead of once per changed role.
    const bool any_role_exists = pending_role_may_exist;
    auto is_in_batch = [this] (const std::optional<UUID> & role_id)
    {
        return role_id && pending_role_ids.contains(*role_id);
    };

    auto finish = [this]
    {
        pending_role_ids.clear();
        pending_role_may_exist = false;
        unreconciled_role_ids.clear();
    };

    // The mappings are keyed by the role names as they are configured or mapped from LDAP, and these names are resolved
    // to the IDs of roles with `access_control.find<Role>`. A name can be resolved to a role with another name, e.g.
    // `UsersConfigAccessStorage` resolves the legacy escaped name `my\.role` to the role `my.role` defined in `users.xml`.
    // So the name from the notification is not compared with the configured or mapped names: instead, every name which
    // is or may become related to the changed role is resolved again, and the resolved ID is granted without any
    // further lookups by name.
    std::set<String> role_names(common_role_names.begin(), common_role_names.end());
    for (const auto & [role_name, user_names] : users_per_roles)
        role_names.insert(role_name);
    for (const auto & [role_name, role_id] : granted_role_ids)
        role_names.insert(role_name);
    for (const auto & [role_name, role_ids] : unreconciled_role_ids)
        role_names.insert(role_name);

    // The new resolution of the names is prepared separately and replaces `granted_role_ids` only after the users
    // are updated, so a failed reconciliation does not lose the IDs of the roles which have to be revoked.
    auto new_granted_role_ids = granted_role_ids;

    std::set<UUID> changed_role_ids;
    std::set<String> changed_role_names;
    for (const auto & role_name : role_names)
    {
        std::optional<UUID> old_role_id;
        if (auto it = granted_role_ids.find(role_name); it != granted_role_ids.end())
            old_role_id = it->second;

        // A failed reconciliation could have granted the roles this name was resolved to at that time to a part of
        // the users. `granted_role_ids` does not know about these roles, so such a name is always processed,
        // even if it is not resolved to any role neither before nor after the current changes.
        const auto unreconciled_it = unreconciled_role_ids.find(role_name);
        const bool is_unreconciled = unreconciled_it != unreconciled_role_ids.end();

        // The removal of a role cannot change the resolution of a name which was not resolved to that role.
        if (!any_role_exists && !is_in_batch(old_role_id) && !is_unreconciled)
            continue;

        // A name already resolved to a changed role is processed too: it could have been resolved during the login
        // of another user after the role had been added but before this notification was delivered, so some users
        // of this name may still lack the role.
        const auto new_role_id = access_control.find<Role>(role_name);
        if (!is_in_batch(old_role_id) && !is_in_batch(new_role_id) && !is_unreconciled)
            continue;

        if (new_role_id)
            new_granted_role_ids.insert_or_assign(role_name, *new_role_id);
        else
            new_granted_role_ids.erase(role_name);

        if (old_role_id)
            changed_role_ids.insert(*old_role_id);
        if (new_role_id)
            changed_role_ids.insert(*new_role_id);
        if (is_unreconciled)
            changed_role_ids.insert(unreconciled_it->second.begin(), unreconciled_it->second.end());
        changed_role_names.insert(role_name);
    }

    if (changed_role_names.empty())
    {
        finish();
        return;
    }

    // Build a list of ids of the relevant users.
    std::vector<UUID> user_ids;
    const bool common_role_changed = std::any_of(changed_role_names.begin(), changed_role_names.end(),
        [this] (const String & role_name) { return common_role_names.contains(role_name); });

    if (common_role_changed)
    {
        user_ids = memory_storage.findAll<User>();
    }
    else
    {
        std::set<String> relevant_user_names;
        for (const auto & role_name : changed_role_names)
        {
            if (const auto it = users_per_roles.find(role_name); it != users_per_roles.end())
                relevant_user_names.insert(it->second.begin(), it->second.end());
        }

        for (const auto & user_name : relevant_user_names)
        {
            if (const auto user_id = memory_storage.find<User>(user_name))
                user_ids.emplace_back(*user_id);
        }
    }

    // Update the granted roles of the relevant users. A user keeps a role if any of its configured or mapped names
    // is still resolved to that role.
    size_t num_updated_users = 0;
    auto update_func = [this, &changed_role_ids, &new_granted_role_ids, &num_updated_users] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
    {
        if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
        {
            if (num_updated_users)
            {
                fiu_do_on(FailPoints::ldap_access_storage_fail_role_reconciliation_after_first_user,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED,
                        "Injected failure of the reconciliation of LDAP role changes after the update of the first user");
                });
            }
            ++num_updated_users;

            auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());

            std::set<UUID> resolved_role_ids;
            auto add_resolved_role_id = [&] (const String & role_name)
            {
                if (const auto it = new_granted_role_ids.find(role_name); it != new_granted_role_ids.end())
                    resolved_role_ids.insert(it->second);
            };

            for (const auto & role_name : common_role_names)
                add_resolved_role_id(role_name);
            if (const auto it = roles_per_users.find(user->getName()); it != roles_per_users.end())
            {
                for (const auto & role_name : it->second)
                    add_resolved_role_id(role_name);
            }

            // Only the roles of this user are visited, not the whole batch of changed roles: otherwise reloading many
            // roles mapped to different users would perform a quadratic number of grants and revokes.
            std::vector<UUID> role_ids_to_revoke;
            for (const auto & role_id : user->granted_roles.getGranted())
            {
                if (changed_role_ids.contains(role_id) && !resolved_role_ids.contains(role_id))
                    role_ids_to_revoke.push_back(role_id);
            }
            changed_user->granted_roles.revoke(role_ids_to_revoke);

            for (const auto & role_id : resolved_role_ids)
            {
                if (changed_role_ids.contains(role_id))
                    changed_user->granted_roles.grant(role_id);
            }
            return changed_user;
        }
        return entity_;
    };

    fiu_do_on(FailPoints::ldap_access_storage_fail_role_reconciliation,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure of the reconciliation of LDAP role changes");
    });

    // Remember the roles which are going to be granted before updating the first user. If only a part of the users
    // is updated before a failure, these roles are already granted to them, while `granted_role_ids` still holds
    // the previous resolution of the names. The next attempts have to visit the users of these names and revoke
    // these roles if the names are resolved differently by then, e.g. if the role is renamed before the retry.
    // Updating a user again is harmless, so the next attempt just processes all the users once more.
    for (const auto & role_name : changed_role_names)
    {
        if (const auto it = new_granted_role_ids.find(role_name); it != new_granted_role_ids.end())
            unreconciled_role_ids[role_name].insert(it->second);
    }

    memory_storage.update(user_ids, update_func);

    granted_role_ids.swap(new_granted_role_ids);
    finish();
}


void LDAPAccessStorage::assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles) const
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
                it = granted_role_ids.insert_or_assign(role_name, *role_id).first;
        }
        else if (pending_role_ids.contains(it->second))
        {
            // The role this name is resolved to has changed, but the change is not reconciled yet (see `processRoleChanges`),
            // so the cached ID can be outdated. It is kept for the reconciliation, and the name is resolved again here.
            if (const auto role_id = access_control.find<Role>(role_name))
                granted_roles.grant(*role_id);
            else
                LOG_WARNING(getLogger(), "Unable to grant {} role '{}' to user '{}': role not found", (common ? "common" : "mapped"), role_name, user_name);
            return;
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

    users_external_roles.erase(user_name);
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

        granted_role_ids.erase(old_role_name);
    }

    // Actualize roles_per_users mapping and users_external_roles cache.
    if (local_role_names.empty())
        roles_per_users.erase(user_name);
    else
        roles_per_users[user_name] = std::move(local_role_names);

    users_external_roles[user_name] = external_roles;
}


void LDAPAccessStorage::updateAssignedRolesNoLock(const UUID & id, const String & user_name, const LDAPClient::SearchResultsList & external_roles) const
{
    // Map and grant the roles from scratch only if the list of external role has changed.
    const auto it = users_external_roles.find(user_name);
    if (it != users_external_roles.end() && it->second == external_roles)
        return;

    auto update_func = [this, &external_roles] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
    {
        if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
        {
            auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
            assignRolesNoLock(*changed_user, external_roles);
            return changed_user;
        }
        return entity_;
    };

    memory_storage.update(id, update_func);
}


std::set<String> LDAPAccessStorage::mapExternalRolesNoLock(const LDAPClient::SearchResultsList & external_roles) const
{
    std::set<String> role_names;

    // If this node can't access LDAP server (or has not privileges to fetch roles) and gets empty list of external roles
    if (external_roles.empty())
        return role_names;

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
    const ClientInfo & /*client_info*/,
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
            throwNotFound(AccessEntityType::USER, credentials.getUserName(), getStorageName());
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
        return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::LDAP), .user_name = credentials.getUserName() };
    return std::nullopt;
}

}
