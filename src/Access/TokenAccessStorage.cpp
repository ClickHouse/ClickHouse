#include <Access/TokenAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
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

namespace
{
    struct ParsedTransform
    {
        String pattern;
        String replacement;
        bool global;
    };

    /// Unescape a string segment
    String unescapeSegment(const String & str, size_t start, size_t end)
    {
        String result;
        result.reserve(end - start);
        bool escaped = false;

        for (size_t i = start; i < end; ++i)
        {
            if (escaped)
            {
                result += str[i];
                escaped = false;
            }
            else if (str[i] == '\\')
                escaped = true;
            else
                result += str[i];
        }

        return result;
    }

    /// Parse sed-style transform pattern: s/pattern/replacement/flags
    ParsedTransform parseSedTransform(const String & transform)
    {
        if (transform.size() < 4 || transform[0] != 's' || transform[1] != '/')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid roles_transform format. Expected sed-style pattern like 's/pattern/replacement/g'");
        }

        bool escaped = false;
        size_t first_slash = 1;
        size_t second_slash = String::npos;
        size_t third_slash = String::npos;

        // Find delimiters using simple state machine
        for (size_t i = first_slash + 1; i < transform.size(); ++i)
        {
            if (escaped)
            {
                escaped = false;
                continue;
            }

            if (transform[i] == '\\')
            {
                escaped = true;
                continue;
            }

            if (transform[i] == '/')
            {
                if (second_slash == String::npos)
                    second_slash = i;
                else if (third_slash == String::npos)
                    third_slash = i;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid roles_transform format. Too many unescaped slashes. Expected sed-style pattern like 's/pattern/replacement/g'");
            }
        }

        if (second_slash == String::npos || third_slash == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid roles_transform format. Expected sed-style pattern like 's/pattern/replacement/g'");

        ParsedTransform result;

        result.pattern = unescapeSegment(transform, first_slash + 1, second_slash);

        size_t replacement_end = (third_slash != String::npos) ? third_slash : transform.size();
        result.replacement = unescapeSegment(transform, second_slash + 1, replacement_end);

        String flags = transform.substr(third_slash + 1);
        result.global = (flags.find('g') != String::npos);

        return result;
    }

    String applyTransform(const String & input, const re2::RE2 & re, const String & replacement, bool global)
    {
        /// `re` is precompiled at storage construction (the constructor refuses
        /// to load with an invalid pattern, so by the time we get here the
        /// regex is guaranteed to be `ok()`). No per-call recompilation; no
        /// silent no-op on a bad pattern.
        String result = input;
        if (global)
            RE2::GlobalReplace(&result, re, replacement);
        else
            RE2::Replace(&result, re, replacement);
        return result;
    }
}

TokenAccessStorage::TokenAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config_, const String & prefix_)
        : IAccessStorage(storage_name_), access_control(access_control_), config(config_), prefix(prefix_),
        memory_storage(storage_name_, access_control.getChangesNotifier(), false)
{
    std::lock_guard lock(mutex);

    const String prefix_str = (prefix.empty() ? "" : prefix + ".");

    if (config.has(prefix_str + "roles_filter"))
    {
        const String filter_pattern = config.getString(prefix_str + "roles_filter");
        roles_filter.emplace(filter_pattern);

        /// Fail closed on invalid regex. RE2 does not throw on bad patterns -- it
        /// constructs an object with ok()==false and silently fails every match.
        /// Reject the configuration up front so the
        /// storage cannot be instantiated in a permissive state.
        if (!roles_filter->ok())
        {
            const String error = roles_filter->error();
            roles_filter.reset();
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid 'roles_filter' regex for Token user directory '{}': {}. "
                            "Refusing to start with a misconfigured filter to avoid granting "
                            "all token groups as roles.",
                            storage_name_, error);
        }
    }

    if (config.has(prefix_str + "roles_transform"))
    {
        String transform = config.getString(prefix_str + "roles_transform");
        ParsedTransform parsed = parseSedTransform(transform);

        /// Compile and validate the regex up front. If we deferred compilation
        /// to runtime (the previous behavior), an invalid regex would silently
        /// return the input unchanged on every call -- meaning every role name
        /// from the IdP would flow into role-mapping ungroomed, defeating the
        /// purpose of `roles_transform`. Fail loudly at construction so the
        /// misconfiguration is visible at startup.
        if (!parsed.pattern.empty())
        {
            roles_transform_pattern.emplace(parsed.pattern);
            if (!roles_transform_pattern->ok())
            {
                const String error = roles_transform_pattern->error();
                roles_transform_pattern.reset();
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Invalid 'roles_transform' regex for Token user directory '{}': {}. "
                                "Refusing to start with a misconfigured transform to avoid admitting "
                                "ungroomed role names from the IdP.",
                                storage_name_, error);
            }
        }
        roles_transform_replacement = parsed.replacement;
        roles_transform_global = parsed.global;
    }

    /// Explicit `roles_mapping` entries are read as a list of <map><from>X</from><to>Y</to></map>
    /// children. The mapping rewrites incoming group names BEFORE `roles_filter` / `roles_transform`,
    /// so each subsequent stage operates on the mapped value. Groups not listed here pass through
    /// to filter/transform unchanged.
    if (config.has(prefix_str + "roles_mapping"))
    {
        Poco::Util::AbstractConfiguration::Keys map_keys;
        config.keys(prefix_str + "roles_mapping", map_keys);

        for (const auto & key : map_keys)
        {
            const String entry_prefix = prefix_str + "roles_mapping." + key;
            if (!config.has(entry_prefix + ".from") || !config.has(entry_prefix + ".to"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "roles_mapping entry '{}' must contain both 'from' and 'to' subelements", key);

            const String from = config.getString(entry_prefix + ".from");
            const String to = config.getString(entry_prefix + ".to");

            if (from.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "roles_mapping entry '{}': 'from' must not be empty", key);
            if (to.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "roles_mapping entry '{}': 'to' must not be empty", key);

            auto [it, inserted] = roles_mapping.emplace(from, to);
            if (!inserted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "roles_mapping has duplicate 'from' value '{}' (already mapped to '{}', cannot remap to '{}')",
                                from, it->second, to);
        }
    }

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

    if (config.has(prefix_str + "default_profile"))
        default_profile_name = config.getString(prefix_str + "default_profile");

    /// Optional IP allowlist for auto-provisioned users. Mirrors the
    /// `users.xml` `<networks>` shape: `<ip>SUBNET</ip>` /
    /// `<host>NAME</host>` / `<host_regexp>REGEX</host_regexp>` children.
    /// Without this, every auto-created token user defaults to `AnyHost` and
    /// admins have no way to restrict token-auth by network through standard
    /// access-control config.
    const auto networks_config_path = prefix_str + "networks";
    if (config.has(networks_config_path))
    {
        AllowedClientHosts hosts;
        Poco::Util::AbstractConfiguration::Keys network_keys;
        config.keys(networks_config_path, network_keys);
        for (const String & key : network_keys)
        {
            const String value = config.getString(networks_config_path + "." + key);
            if (key.starts_with("ip"))
                hosts.addSubnet(value);
            else if (key.starts_with("host_regexp"))
                hosts.addNameRegexp(value);
            else if (key.starts_with("host"))
                hosts.addName(value);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Token user directory '{}': unknown <networks> entry '{}'; expected 'ip', 'host', or 'host_regexp'.",
                                storage_name_, key);
        }
        auto_user_allowed_hosts = std::move(hosts);
    }

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

    /// Interserver hops authenticate with the cluster secret and then trust
    /// `initial_user` via AlwaysAllowCredentials (see TCPHandler). Mirror LDAP.
    if (typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

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

void TokenAccessStorage::assignProfileNoLock(User & user) const
{
    if (default_profile_name.empty())
        return;

    const auto & user_name = user.getName();
    auto & settings = user.settings;

    // Look up the profile ID once
    const auto profile_id = access_control.find<SettingsProfile>(default_profile_name);
    if (!profile_id)
    {
        LOG_TRACE(getLogger(), "Did not assign profile '{}' to user '{}': profile not found", default_profile_name, user_name);
        return;
    }

    // Check if profile is already assigned
    bool profile_already_assigned = false;
    for (const auto & element : settings)
    {
        if (element.parent_profile.has_value() && element.parent_profile == *profile_id)
        {
            profile_already_assigned = true;
            break;
        }
    }

    if (!profile_already_assigned)
    {
        SettingsProfileElement profile_element;
        profile_element.parent_profile = *profile_id;
        settings.push_back(std::move(profile_element));
        LOG_TRACE(getLogger(), "Assigned profile '{}' to user '{}'", default_profile_name, user_name);
    }
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
    std::unique_lock lock(mutex);

    /// Accept TokenCredentials (normal JWT login) and AlwaysAllowCredentials
    /// (interserver hop after cluster-secret verification). Reject every other
    /// credential type BEFORE any reference-form `typeid_cast` that would throw
    /// a `LOGICAL_ERROR`. `MultipleAccessStorage::authenticateImpl` does not
    /// catch per-storage exceptions -- so a single Basic / SSL-cert / Kerberos /
    /// SSH login attempt would abort authentication for every later storage in
    /// `user_directories`. Concretely, listing `<token>` ahead of `<users.xml>`
    /// would lock out every Basic-auth user. Return nullopt cleanly, matching
    /// the LDAP-side idiom in `LDAPAccessStorage::areLDAPCredentialsValidNoLock`.
    const auto * always_allow_credentials = dynamic_cast<const AlwaysAllowCredentials *>(&credentials);
    const auto * token_credentials_ptr = dynamic_cast<const TokenCredentials *>(&credentials);
    if (!always_allow_credentials && !token_credentials_ptr)
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, credentials.getUserName(), getStorageName());
        return {};
    }

    auto id = memory_storage.find<User>(credentials.getUserName());
    UserPtr user = id ? memory_storage.read<User>(*id) : nullptr;

    std::shared_ptr<User> new_user;
    if (!user)
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        new_user = std::make_shared<User>();
        new_user->setName(credentials.getUserName());
        new_user->authentication_methods.emplace_back(AuthenticationType::JWT);
        /// Stamp the storage's pinned processor onto the auth method so the
        /// per-request validity check (`Session::checkIfUserIsStillValid`)
        /// can detect when an admin removes that processor and terminate
        /// active sessions whose tokens were issued through it (M-28).
        new_user->authentication_methods.back().setTokenProcessorName(provider_name);
        /// If the operator configured a network allowlist for this storage,
        /// stamp it onto the auto-created user so `isAddressAllowed` checks it
        /// below. Without this, every auto-provisioned token user inherits
        /// `AnyHostTag` and there is no way to restrict token auth by network.
        if (auto_user_allowed_hosts.has_value())
            new_user->allowed_client_hosts = *auto_user_allowed_hosts;
        user = new_user;
    }

    if (!isAddressAllowed(*user, address))
        throwAddressNotAllowed(address);

    /// Interserver mode: TCPHandler already verified the cluster secret and is
    /// trusting `client_info.initial_user`.
    if (always_allow_credentials)
    {
        if (new_user)
        {
            /// TODO: mapped roles from the JWT are not available here without
            /// the bearer token; grant common roles only. Session applies the
            /// roles pushed by the initiator.
            assignRolesNoLock(*new_user, /* external_roles= */ {});
            assignProfileNoLock(*new_user);
            id = memory_storage.insert(new_user);

            lock.unlock();
            access_control.getChangesNotifier().sendNotifications();
        }

        if (id)
            return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::JWT), .user_name = user->getName() };
        return std::nullopt;
    }

    const auto & token_credentials = *token_credentials_ptr;

    if (!external_authenticators.checkTokenCredentials(token_credentials, provider_name))
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, credentials.getUserName(), getStorageName());

        return {};
    }

    /// Pipeline: incoming group --(roles_mapping)--> mapped name --(roles_filter)--> kept/dropped --(roles_transform)--> CH role name.
    /// Each stage is independent and optional; groups absent from `roles_mapping` pass through unchanged.
    std::set<String> external_roles;

    /// Defensive: a broken filter regex must NEVER fall through to the permissive
    /// "grant everything that survives the rest of the pipeline" branch. Parse-time
    /// validation in the constructor already rejects invalid patterns; this guard
    /// preserves the invariant in case any future code path constructs the filter
    /// without the parse-time check (e.g. config reload).
    if (roles_filter.has_value() && !roles_filter->ok())
    {
        LOG_ERROR(getLogger(),
                  "{}: Configured 'roles_filter' is invalid ('{}'); refusing to map any "
                  "external roles for user '{}' to avoid granting all token groups.",
                  getStorageName(), roles_filter->error(), credentials.getUserName());
    }
    else
    {
        const bool has_filter = roles_filter.has_value();
        const bool has_transform = roles_transform_pattern.has_value() && roles_transform_replacement.has_value();

        for (const auto & group : token_credentials.getGroups())
        {
            String name = group;

            if (!roles_mapping.empty())
            {
                const auto it = roles_mapping.find(group);
                if (it != roles_mapping.end())
                {
                    name = it->second;
                    LOG_TRACE(getLogger(), "{}: Mapped group '{}' to '{}'", getStorageName(), group, name);
                }
            }

            if (has_filter && !RE2::FullMatch(name, roles_filter.value()))
            {
                LOG_TRACE(getLogger(), "{}: Group '{}' (after mapping) did not match roles_filter, skipping", getStorageName(), name);
                continue;
            }

            if (has_transform)
            {
                String transformed = applyTransform(name, roles_transform_pattern.value(), roles_transform_replacement.value(), roles_transform_global);
                if (transformed != name)
                {
                    LOG_TRACE(getLogger(), "{}: Transformed '{}' to '{}'", getStorageName(), name, transformed);
                    name = std::move(transformed);
                }
            }

            external_roles.insert(name);
            LOG_TRACE(getLogger(), "{}: Granted role (group) {} to user", getStorageName(), name);
        }
    }

    if (new_user)
    {
        assignRolesNoLock(*new_user, external_roles);
        assignProfileNoLock(*new_user);
        id = memory_storage.insert(new_user);
    }
    else
    {
        /// Apply role-set and profile changes atomically under a single
        /// `memory_storage.update`. Splitting them into two separate updates
        /// (the prior shape) opened a reader-observable window between
        /// "new roles, old profile" and "new roles, new profile" -- a query
        /// from another thread that read the user via `AccessControl::read`
        /// would observe a mid-state, since `MemoryAccessStorage`'s lock is
        /// independent of `TokenAccessStorage::mutex` (M-31).
        ///
        /// Preserve the existing early-return optimization: skip the update
        /// when external_roles haven't changed AND the profile is already
        /// assigned. The `assignRolesNoLock` cleanup still has to run if
        /// the role set changes, so it lives inside the update lambda.
        const bool roles_changed = [&]
        {
            const auto it = user_external_roles.find(user->getName());
            return it == user_external_roles.end() || it->second != external_roles;
        }();

        if (roles_changed)
        {
            memory_storage.update(*id, [this, &external_roles] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
            {
                if (auto user_entity = typeid_cast<std::shared_ptr<const User>>(entity_))
                {
                    auto changed_user = typeid_cast<std::shared_ptr<User>>(user_entity->clone());
                    assignRolesNoLock(*changed_user, external_roles);
                    assignProfileNoLock(*changed_user);
                    return changed_user;
                }
                return entity_;
            });
        }
        else
        {
            /// Roles are stable; just refresh the profile in case it was
            /// added/changed in config since the last auth.
            memory_storage.update(*id, [this] (const AccessEntityPtr & entity_, const UUID &) -> AccessEntityPtr
            {
                if (auto user_entity = typeid_cast<std::shared_ptr<const User>>(entity_))
                {
                    auto changed_user = typeid_cast<std::shared_ptr<User>>(user_entity->clone());
                    assignProfileNoLock(*changed_user);
                    return changed_user;
                }
                return entity_;
            });
        }
    }

    /// Flush queued user-entity events from this storage's `memory_storage` so
    /// subscribers observe the freshly-resolved roles and profile right away.
    ///
    /// `memory_storage.insert` / `update` only enqueue `onEntityAdded` /
    /// `onEntityUpdated` on the shared `AccessChangesNotifier`; without an
    /// explicit `sendNotifications` they sit on the queue until some unrelated
    /// access mutation (a SQL DDL on access entities, a config reload, a
    /// replicated-storage sync) happens to trigger a drain. During that window
    /// any existing `ContextAccess` bound to this user UUID keeps serving its
    /// previously-cached authorization state -- a freshly-revoked role appears
    /// "still granted" until the next unrelated trigger.
    ///
    /// Note: `applyRoleChangeNoLock` (the storage's other mutation site) does
    /// NOT need an explicit flush -- it only runs inside `processRoleChange`,
    /// which is itself dispatched from a `sendNotifications` drain; the events
    /// it queues are picked up by the very loop that called it. Only
    /// `authenticateImpl` runs outside of any drain and so is the one site
    /// that has to flush explicitly.
    /// Release `mutex` first: the notifier drain re-enters this storage via
    /// `processRoleChange` (subscribed for Role changes) while holding the
    /// notifier's `sending_notifications`, so holding both in opposite order
    /// here would deadlock (tsan lock-order-inversion vs. CREATE ROLE).
    lock.unlock();
    access_control.getChangesNotifier().sendNotifications();

    if (id)
        return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::JWT), .user_name = user->getName() };
    return std::nullopt;
}


}
