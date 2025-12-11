#include <Access/TokenAccessStorage.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/container_hash/hash.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

TokenAccessStorage::TokenAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config_, const String & prefix_)
        : IAccessStorage(storage_name_), access_control(access_control_), config(config_), prefix(prefix_),
        memory_storage(storage_name_, access_control.getChangesNotifier(), false)
{
    std::lock_guard lock(mutex);

    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

    if (config.has(prefix_str + "roles_filter"))
        roles_filter.emplace(config.getString(prefix_str + "roles_filter"));

    provider_name = config.getString(prefix_str + "processor");
    if (provider_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'processor' must be specified for Token user directory");

    std::set<String> common_roles_cfg;
    if (config.has(prefix_str + "common_roles"))
    {
        Poco::Util::AbstractConfiguration::Keys role_names;
        config.keys(prefix_str + "common_roles", role_names);

        common_roles_cfg.insert(role_names.begin(), role_names.end());
    }
    common_role_names.swap(common_roles_cfg);

    user_external_roles.clear();
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

void TokenAccessStorage::applyRoleChangeNoLock(bool grant, const UUID & role_id, const String & role_name)
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

void TokenAccessStorage::processRoleChange(const UUID & id, const AccessEntityPtr & entity)
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

const char * TokenAccessStorage::getStorageType() const
{
    return STORAGE_TYPE;
}

bool TokenAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock(mutex);
    return memory_storage.exists(id);
}

String TokenAccessStorage::getStorageParamsJSON() const
{
    std::lock_guard lock(mutex);
    Poco::JSON::Object params_json;

    params_json.set("provider", provider_name);

    Poco::JSON::Array common_role_names_json;
    for (const auto & role : common_role_names)
    {
        common_role_names_json.add(role);
    }
    params_json.set("roles", common_role_names_json);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(params_json, oss);

    return oss.str();
}

bool TokenAccessStorage::areTokenCredentialsValidNoLock(const User & user, const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const
{
    if (!credentials.isReady())
        return false;

    if (credentials.getUserName() != user.getName())
        return false;

    if (const auto * token_credentials = dynamic_cast<const TokenCredentials *>(&credentials))
        return external_authenticators.checkTokenCredentials(*token_credentials);

    return false;
}

std::optional<UUID> TokenAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock(mutex);
    return memory_storage.find(type, name);
}


std::vector<UUID> TokenAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock(mutex);
    return memory_storage.findAll(type);
}

AccessEntityPtr TokenAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock(mutex);
    return memory_storage.read(id, throw_if_not_exists);
}

std::optional<std::pair<String, AccessEntityType>> TokenAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock(mutex);
    return memory_storage.readNameWithType(id, throw_if_not_exists);
}

void TokenAccessStorage::assignRolesNoLock(User & user, const std::set<String> & external_roles) const
{
    const auto & user_name = user.getName();
    auto & granted_roles = user.granted_roles;

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
            LOG_TRACE(getLogger(), "Did not grant {} role '{}' to user '{}': role not found", (common ? "common" : "mapped"), role_name, user_name);
        }
    };

    user_external_roles.erase(user_name);
    granted_roles = {};
    const auto old_role_names = std::move(roles_per_users[user_name]);

    // Grant the common roles first.
    for (const auto & role_name : common_role_names)
    {
        grant_role(role_name, true /* common */);
    }

    // Grant the mapped external roles and actualize users_per_roles mapping.
    // external_roles allowed to overlap with common_role_names.
    for (const auto & role_name : external_roles)
    {
        grant_role(role_name, false /* mapped */);
        users_per_roles[role_name].insert(user_name);
    }

    // Cleanup users_per_roles and granted_role_* mappings.
    for (const auto & old_role_name : old_role_names)
    {
        if (external_roles.contains(old_role_name))
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

    // Actualize roles_per_users mapping and user_external_roles cache.
    if (external_roles.empty())
        roles_per_users.erase(user_name);
    else
        roles_per_users[user_name] = external_roles;

    user_external_roles[user_name] = external_roles;
}

void TokenAccessStorage::updateAssignedRolesNoLock(const UUID & id, const String & user_name, const std::set<String> & external_roles) const
{
    // Map and grant the roles from scratch only if the list of external role has changed.
    const auto it = user_external_roles.find(user_name);
    if (it != user_external_roles.end() && it->second == external_roles)
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


std::optional<AuthResult> TokenAccessStorage::authenticateImpl(
        const Credentials & credentials,
        const Poco::Net::IPAddress & address,
        const ExternalAuthenticators & external_authenticators,
        const ClientInfo & /* client_info */,
        bool throw_if_user_not_exists,
        bool /* allow_no_password */,
        bool /* allow_plaintext_password */) const
{
    std::lock_guard lock(mutex);
    auto id = memory_storage.find<User>(credentials.getUserName());
    UserPtr user = id ? memory_storage.read<User>(*id) : nullptr;

    const auto & token_credentials = typeid_cast<const TokenCredentials &>(credentials);

    if (!external_authenticators.checkTokenCredentials(token_credentials, provider_name))
    {
        // Even though token itself may be valid (especially in case of a jwt token), authentication has just failed.
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, credentials.getUserName(), getStorageName());

        return {};
    }

    std::shared_ptr<User> new_user;
    if (!user)
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        new_user = std::make_shared<User>();
        new_user->setName(credentials.getUserName());
        new_user->authentication_methods.emplace_back(AuthenticationType::JWT);
        user = new_user;
    }

    if (!isAddressAllowed(*user, address))
        throwAddressNotAllowed(address);

    std::set<String> external_roles;
    if (roles_filter.has_value() && roles_filter.value().ok())
    {
        LOG_TRACE(getLogger(), "{}: External role filter found, applying only matching groups", getStorageName());
        for (const auto & group: token_credentials.getGroups()) {
            if (RE2::FullMatch(group, roles_filter.value()))
            {
                external_roles.insert(group);
                LOG_TRACE(getLogger(), "{}: Granted role (group) {} to user", getStorageName(), user->getName());
            }
        }
    }
    else
    {
        LOG_TRACE(getLogger(), "{}: No external role filtering set, applying all available groups", getStorageName());
        external_roles = token_credentials.getGroups();
    }

    if (new_user)
    {
        assignRolesNoLock(*new_user, external_roles);
        id = memory_storage.insert(new_user);
    }
    else
    {
        // Just in case external_roles are changed.
        updateAssignedRolesNoLock(*id, user->getName(), external_roles);
    }

    if (id)
        return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::JWT) };
    return std::nullopt;
}


}
