#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Credentials.h>
#include <Access/LDAPClient.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>
#include <base/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <fmt/ranges.h>
#include <algorithm>
#include <cmath>
#include <random>
#include <sstream>
#include <string_view>


namespace ProfileEvents
{
    extern const Event LDAPSyncRuns;
    extern const Event LDAPSyncFailures;
    extern const Event LDAPSyncUsersAdded;
    extern const Event LDAPSyncUsersUpdated;
    extern const Event LDAPSyncUsersRemoved;
    extern const Event LDAPSyncUsersShadowed;
    extern const Event LDAPSyncUsersExcluded;
    extern const Event LDAPSyncRolesCreated;
    extern const Event LDAPSyncRolesMissing;
}

namespace CurrentMetrics
{
    extern const Metric LDAPSyncRunning;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LDAP_ERROR;
    extern const int WRONG_PASSWORD;
}

namespace
{

/// Seconds of the steady clock, never 0, so that 0 can mean "never" in the atomics below.
Int64 steadyNowSeconds()
{
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    return std::max<Int64>(1, now);
}

}

LDAPAccessStorage::LDAPAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
    : IAccessStorage(storage_name_), access_control(access_control_), memory_storage(storage_name_, access_control.getChangesNotifier(), false)
{
    setConfiguration(config, prefix);
}


LDAPAccessStorage::~LDAPAccessStorage()
{
    /// `AccessControl::shutdown` has normally stopped the synchronisation thread already; a destructor
    /// must not throw, so a failure to join here can only be logged (same as `ZooKeeperReplicator`).
    try
    {
        stopSyncThread();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
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
    const bool has_exclude_users = config.has(prefix_str + "exclude_users");
    const bool has_sync = config.has(prefix_str + "sync");

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

    std::set<String> excluded_user_names_cfg;
    if (has_exclude_users)
    {
        Poco::Util::AbstractConfiguration::Keys exclude_users_keys;
        config.keys(prefix_str + "exclude_users", exclude_users_keys);
        for (const auto & key : exclude_users_keys)
        {
            // Only `<user>` entries are meaningful here; anything else is most likely a typo that would
            // silently exclude nobody, so it is rejected instead of ignored.
            if (key != "user" && !key.starts_with("user["))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unexpected key '{}' in exclude_users for LDAP user directory, only 'user' entries are allowed", key);

            const auto excluded_user_name = config.getString(prefix_str + "exclude_users." + key);
            if (excluded_user_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty user name in exclude_users for LDAP user directory");

            excluded_user_names_cfg.insert(excluded_user_name);
        }
    }

    std::optional<SyncParams> sync_params_cfg;
    if (has_sync)
        sync_params_cfg = parseSyncParams(config, prefix_str + "sync", role_search_params_cfg);

    ldap_server_name = ldap_server_name_cfg;
    role_search_params.swap(role_search_params_cfg);
    common_role_names.swap(common_roles_cfg);
    excluded_user_names.swap(excluded_user_names_cfg);
    sync_params = std::move(sync_params_cfg);

    users_external_roles.clear();
    users_per_roles.clear();
    roles_per_users.clear();
    granted_role_names.clear();
    granted_role_ids.clear();
    synced_user_names.clear();

    role_change_subscription = access_control.subscribeForChanges<Role>(
        [this] (const std::vector<AccessChangesNotifier::Change> & changes)
        {
            for (const auto & change : changes)
                this->processRoleChange(change.id, change.entity);
        }
    );
}


LDAPAccessStorage::SyncParams LDAPAccessStorage::parseSyncParams(
    const Poco::Util::AbstractConfiguration & config, const String & prefix, const LDAPClient::RoleSearchParamsList & role_search_params)
{
    /// Every guard below has a default, so a misspelt key would silently leave the default in place; reject it instead.
    static const std::set<String> known_keys{
        "interval", "base_dn", "scope", "search_filter", "attribute", "page_size", "create_roles", "roles_storage",
        "only_synced_users", "min_users", "max_users", "max_removed_fraction", "max_staleness", "dry_run"};

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        if (!known_keys.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown entry '{}' in '{}' section of LDAP user directory", key, prefix);
    }

    SyncParams params;
    parseLDAPUserEnumerationParams(params.enumeration, config, prefix);

    /// Both durations are kept as `std::chrono::seconds`, whose count is signed: a value that does not fit would
    /// wrap into a negative wait, so the thread would spin against the directory and the jitter range would be
    /// inverted. Ten years is far beyond any meaningful value; reject the rest at startup.
    static constexpr UInt64 max_duration_s = 10ULL * 365 * 24 * 3600;
    auto get_duration = [&](const char * key, std::chrono::seconds default_value)
    {
        const UInt64 value_s = config.getUInt64(prefix + "." + key, default_value.count());
        if (value_s > max_duration_s)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'{}' in '{}' section must not exceed {} s (ten years), got {}", key, prefix, max_duration_s, value_s);
        return std::chrono::seconds{value_s};
    };

    params.interval = get_duration("interval", params.interval);
    params.create_roles = config.getBool(prefix + ".create_roles", params.create_roles);
    params.only_synced_users = config.getBool(prefix + ".only_synced_users", params.only_synced_users);
    params.min_users = config.getUInt64(prefix + ".min_users", params.min_users);
    params.max_users = config.getUInt64(prefix + ".max_users", params.max_users);
    params.max_removed_fraction = config.getDouble(prefix + ".max_removed_fraction", params.max_removed_fraction);
    params.max_staleness = get_duration("max_staleness", params.max_staleness);
    params.dry_run = config.getBool(prefix + ".dry_run", params.dry_run);

    if (config.has(prefix + ".roles_storage"))
    {
        params.roles_storage = config.getString(prefix + ".roles_storage");
        if (params.roles_storage.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'roles_storage' entry in '{}' section", prefix);
    }

    if (params.max_users != 0 && params.max_users <= params.min_users)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "'max_users' in '{}' section must be 0 (unlimited) or greater than 'min_users' ({}), got {}",
                        prefix, params.min_users, params.max_users);
    params.enumeration.max_entries = params.max_users;

    /// The negated form also rejects NaN.
    if (!(params.max_removed_fraction >= 0.0 && params.max_removed_fraction <= 1.0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "'max_removed_fraction' in '{}' section must be between 0 and 1, got {}", prefix, params.max_removed_fraction);

    if (params.max_staleness > std::chrono::seconds{0})
    {
        /// The staleness gate refuses users that are in the snapshot; with lazily materialised users it could
        /// refuse a user this directory never synchronised, and without a periodic run it would trip forever.
        if (!params.only_synced_users)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'max_staleness' in '{}' section requires 'only_synced_users' = true", prefix);
        if (params.interval == std::chrono::seconds{0})
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'max_staleness' in '{}' section requires a periodic synchronisation ('interval' > 0)", prefix);
        if (params.max_staleness <= params.interval)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'max_staleness' ({} s) in '{}' section must be greater than 'interval' ({} s); at least twice the interval is recommended",
                            params.max_staleness.count(), prefix, params.interval.count());
    }

    if (params.create_roles)
    {
        /// Only allow-listed roles are ever created: the `groups` lists are the complete set of roles the
        /// directory may grant, and creating a role for every group a user belongs to is out of the question.
        const bool has_groups = std::ranges::any_of(role_search_params, [](const auto & mapping) { return !mapping.groups.empty(); });
        if (!has_groups)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'create_roles' in '{}' section requires a non-empty 'groups' allow-list in at least one 'role_mapping' section", prefix);
    }

    return params;
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
            const auto old_role_name = it->second;
            if (new_role_name != old_role_name)
            {
                /// Revoke the old name first; the new name is granted like a role that appeared under it, so that a
                /// rename into a name another storage already defines does not split the role either.
                applyRoleChangeNoLock(false /* revoke */, id, old_role_name);
                grantRoleByNameNoLock(id, new_role_name);
            }
        }
        else // Added a role.
        {
            grantRoleByNameNoLock(id, new_role_name);
        }
    }
    else // Removed a role.
    {
        if (it != granted_role_names.end()) // Removed a granted role.
        {
            const auto old_role_name = it->second;
            applyRoleChangeNoLock(false /* revoke */, id, old_role_name);

            /// Another role of the name may have been shadowed by the removed one; the users get it now, like a
            /// login or a run would grant it.
            if (const auto resolved = access_control.find<Role>(old_role_name))
                applyRoleChangeNoLock(true /* grant */, *resolved, old_role_name);
        }
    }
}


void LDAPAccessStorage::grantRoleByNameNoLock(const UUID & id, const String & role_name)
{
    /// The users of this directory must hold exactly the role the name resolves to, the first one in
    /// `user_directories` order (`AccessControl::find`), which is the one `GRANT ... TO role_name` reaches. A second
    /// role of the name may appear in another storage (a replicated storage refreshed from ZooKeeper, a file loaded
    /// by a disk storage), or the synchronisation may have created its copy before that one appeared; granting every
    /// copy would split the role between the users who have one id and those who get the other.
    const auto resolved = access_control.find<Role>(role_name);
    const auto granted_it = granted_role_ids.find(role_name);
    if (granted_it != granted_role_ids.end() && granted_it->second != id)
    {
        if (!resolved || *resolved == granted_it->second)
            return; /// The copy that appeared is shadowed by the granted one; nothing changes for the users.

        applyRoleChangeNoLock(false /* revoke */, granted_it->second, role_name);
        applyRoleChangeNoLock(true /* grant */, *resolved, role_name);
        return;
    }

    applyRoleChangeNoLock(true /* grant */, resolved.value_or(id), role_name);
}


void LDAPAccessStorage::applyRoleChangeNoLock(bool grant, UUID role_id, const String & role_name)
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

        const auto iit = granted_role_ids.find(old_role_name);
        if (iit == granted_role_ids.end())
            continue;

        const auto old_role_id = iit->second;
        granted_role_names.erase(old_role_id);
        granted_role_ids.erase(iit);
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


void LDAPAccessStorage::removeUserNoLock(const String & user_name) const
{
    /// Mirror of the cleanup `assignRolesNoLock` performs for roles a user lost, for every role of the user.
    users_external_roles.erase(user_name);
    synced_user_names.erase(user_name);
    unverified_user_names.erase(user_name);

    const auto it = roles_per_users.find(user_name);
    if (it == roles_per_users.end())
        return;

    for (const auto & role_name : it->second)
    {
        const auto rit = users_per_roles.find(role_name);
        if (rit == users_per_roles.end())
            continue;

        auto & user_names = rit->second;
        user_names.erase(user_name);

        if (!user_names.empty())
            continue;

        users_per_roles.erase(rit);

        /// Common roles stay granted to every user, so their ids are kept as long as the storage lives.
        if (common_role_names.contains(role_name))
            continue;

        const auto iit = granted_role_ids.find(role_name);
        if (iit == granted_role_ids.end())
            continue;

        granted_role_names.erase(iit->second);
        granted_role_ids.erase(iit);
    }

    roles_per_users.erase(it);
}


std::shared_ptr<User> LDAPAccessStorage::makeUserNoLock(const String & user_name) const
{
    auto user = std::make_shared<User>();
    user->setName(user_name);
    user->authentication_methods.emplace_back(AuthenticationType::LDAP);
    user->authentication_methods.back().setLDAPServerName(ldap_server_name);
    return user;
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
        const auto & role_mapping = role_search_params[i];
        const auto & prefix = role_mapping.prefix;

        for (const auto & external_role : external_role_set)
        {
            /// Pipeline: value -> [DN-form group match | `rdn_attribute` extraction -> plain group match] -> `prefix` -> role name.
            String value;

            bool matched_dn_group = false;
            if (!role_mapping.dn_groups.empty())
            {
                if (const auto normalized_dn = LDAPClient::normalizeDN(external_role))
                {
                    const auto it = role_mapping.dn_groups.find(*normalized_dn);
                    if (it != role_mapping.dn_groups.end())
                    {
                        value = it->second;
                        matched_dn_group = true;
                    }
                }
            }

            if (!matched_dn_group)
            {
                value = external_role;

                if (!role_mapping.rdn_attribute.empty())
                {
                    const auto rdn_value = LDAPClient::extractRDNValue(external_role, role_mapping.rdn_attribute);
                    if (!rdn_value)
                    {
                        LOG_TRACE(getLogger(), "Ignoring role mapping value '{}': not a DN with a '{}' RDN", external_role, role_mapping.rdn_attribute);
                        continue;
                    }
                    value = *rdn_value;
                }

                if (!role_mapping.groups.empty())
                {
                    const auto it = role_mapping.plain_groups.find(toLowerCopyASCII(value));
                    if (it == role_mapping.plain_groups.end())
                    {
                        LOG_TRACE(getLogger(), "Ignoring role mapping value '{}': not in the 'groups' list", external_role);
                        continue;
                    }
                    value = it->second;
                }
            }

            if (prefix.size() < value.size() && value.starts_with(prefix))
            {
                role_names.emplace(value, prefix.size());
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
    {
        /// In a synced directory the synchronisation is the sole role authority, so a login verifies the
        /// password only. A role search here would feed the `verification_cooldown` cache, whose entries can
        /// be up to `verification_cooldown` old, and a cache hit would then re-apply a stale role set over
        /// what the synchronisation just wrote.
        if (sync_params)
            return external_authenticators.checkLDAPCredentials(ldap_server_name, *basic_credentials, nullptr, nullptr);

        return external_authenticators.checkLDAPCredentials(ldap_server_name, *basic_credentials, &role_search_params, &role_search_results);
    }

    return false;
}


void LDAPAccessStorage::checkNotStale(const String & user_name, std::string_view action) const
{
    const auto & params = *sync_params;
    if (params.max_staleness == std::chrono::seconds{0})
        return;

    const Int64 now_s = steadyNowSeconds();
    const Int64 last_success_s = last_sync_success_time_s.load();
    const bool never_synced = (last_success_s == 0);
    const Int64 age_s = never_synced ? 0 : (now_s - last_success_s);
    if (!never_synced && age_s <= params.max_staleness.count())
        return;

    String message;
    if (never_synced)
        message = fmt::format("LDAP directory {} has never been synchronised successfully (max_staleness = {} s), refusing to {} user '{}'",
            backQuote(getStorageName()), params.max_staleness.count(), action, user_name);
    else
        message = fmt::format("LDAP directory {} has not been synchronised for {} s (max_staleness = {} s), refusing to {} user '{}'",
            backQuote(getStorageName()), age_s, params.max_staleness.count(), action, user_name);

    /// During an outage every login of every synced user fails with this; one line per minute is enough.
    Int64 last_log_s = last_staleness_log_time_s.load();
    if (now_s - last_log_s >= 60 && last_staleness_log_time_s.compare_exchange_strong(last_log_s, now_s))
        LOG_WARNING(getLogger(), "{}", message);

    throw Exception(ErrorCodes::LDAP_ERROR, "{}", message);
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
        role_mapping_json.set("rdn_attribute", role_mapping.rdn_attribute);

        Poco::JSON::Array groups_json;
        for (const auto & group : role_mapping.groups)
        {
            groups_json.add(group);
        }
        role_mapping_json.set("groups", groups_json);

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

    Poco::JSON::Array excluded_user_names_json;
    for (const auto & user_name : excluded_user_names)
    {
        excluded_user_names_json.add(user_name);
    }
    params_json.set("exclude_users", excluded_user_names_json);

    if (sync_params)
    {
        /// No secret lives here: the lookup credentials belong to the server definition.
        Poco::JSON::Object sync_json;
        sync_json.set("interval", static_cast<UInt64>(sync_params->interval.count()));
        sync_json.set("base_dn", sync_params->enumeration.base_dn);
        sync_json.set("search_filter", sync_params->enumeration.search_filter);
        sync_json.set("attribute", sync_params->enumeration.attribute);

        String scope;
        switch (sync_params->enumeration.scope)
        {
            case LDAPClient::SearchParams::Scope::BASE:      scope = "base"; break;
            case LDAPClient::SearchParams::Scope::ONE_LEVEL: scope = "one_level"; break;
            case LDAPClient::SearchParams::Scope::SUBTREE:   scope = "subtree"; break;
            case LDAPClient::SearchParams::Scope::CHILDREN:  scope = "children"; break;
        }
        sync_json.set("scope", scope);

        sync_json.set("page_size", sync_params->enumeration.page_size);
        sync_json.set("create_roles", sync_params->create_roles);
        sync_json.set("roles_storage", sync_params->roles_storage);
        sync_json.set("only_synced_users", sync_params->only_synced_users);
        sync_json.set("min_users", static_cast<UInt64>(sync_params->min_users));
        sync_json.set("max_users", static_cast<UInt64>(sync_params->max_users));
        sync_json.set("max_removed_fraction", sync_params->max_removed_fraction);
        sync_json.set("max_staleness", static_cast<UInt64>(sync_params->max_staleness.count()));
        sync_json.set("dry_run", sync_params->dry_run);
        params_json.set("sync", sync_json);
    }

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(params_json, oss);

    return oss.str();
}


std::optional<UUID> LDAPAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock(mutex);

    /// `memory_storage` must never hold a name listed in `exclude_users` (see `authenticateImpl`), so the
    /// answer is known without looking, and both `findImpl` overloads must give it.
    if (type == AccessEntityType::USER && excluded_user_names.contains(name))
        return {};

    return memory_storage.find(type, name);
}


std::optional<UUID> LDAPAccessStorage::findImpl(AccessEntityType type, const String & name, bool force_external_lookup) const
{
    std::lock_guard lock(mutex);

    /// Names listed in `exclude_users` are never resolved through LDAP, not even by the forced lookup
    /// that `EXECUTE AS` performs, and `memory_storage` must never hold them (see `authenticateImpl`).
    /// Decided before touching the memory storage so that this overload can never disagree with the
    /// plain one above.
    if (type == AccessEntityType::USER && excluded_user_names.contains(name))
    {
        if (force_external_lookup)
            LOG_DEBUG(getLogger(), "Skipping excluded user {}: the name is listed in exclude_users", name);
        return {};
    }

    auto id = memory_storage.find(type, name);

    /// Only USER lookups go to LDAP; other entity types (roles, profiles, ...) live
    /// elsewhere and are not resolvable through the LDAP directory.
    if (!force_external_lookup || type != AccessEntityType::USER)
        return id;

    if (sync_params)
    {
        /// The gates of `authenticateImpl`, in the same order: a name in the synchronised snapshot is refused
        /// while the snapshot is stale, and with `only_synced_users` a name outside it does not exist here,
        /// whatever the directory says. Without the staleness gate `EXECUTE AS` would keep resolving a user,
        /// with the roles of the last run, while the same user's password logins are refused as stale.
        /// Roles never come from a lookup in a synced directory, so an existing entry is not refreshed
        /// either, and a lazily materialised one (only without `only_synced_users`) waits for the next run
        /// to get its roles.
        if (id)
        {
            checkNotStale(name, "resolve");
            /// An entry the interserver path materialised (possible without `only_synced_users`) is confirmed
            /// upstream before it is impersonated; the next run removes it anyway if the directory does not
            /// know the name. Roles are not touched: in a synced directory they come from the runs only.
            if (unverified_user_names.contains(name))
            {
                if (!access_control.getExternalAuthenticators().findLDAPUser(ldap_server_name, name, nullptr, nullptr))
                {
                    LOG_DEBUG(getLogger(), "User {} was materialised in LDAP directory {} by an interserver query and the directory does not confirm the name; not resolving it",
                        name, backQuote(getStorageName()));
                    return {};
                }
                unverified_user_names.erase(name);
            }
            return id;
        }

        if (sync_params->only_synced_users)
        {
            LOG_DEBUG(getLogger(), "User {} is not in the synchronised snapshot of directory {}", name, backQuote(getStorageName()));
            return {};
        }

        if (!access_control.getExternalAuthenticators().findLDAPUser(ldap_server_name, name, nullptr, nullptr))
            return {};

        auto new_user = makeUserNoLock(name);
        assignRolesNoLock(*new_user, {});
        return memory_storage.insert(new_user);
    }

    const bool has_role_mapping = !role_search_params.empty();

    /// Two kinds of cached entries are not taken at face value. One was materialised by the interserver
    /// `AlwaysAllowCredentials` path of a distributed query (`unverified_user_names`): the initiator vouched
    /// for the name, the directory never did, so a phantom or an offboarded user could be impersonated
    /// through it. The other has fewer `users_external_roles[name]` entries than `role_search_params`
    /// (a real login always leaves one per search parameter, even if empty), which is what that same path
    /// leaves behind. Both are looked up under the service bind: a name the directory does not confirm is
    /// not resolved, and a confirmed entry gets the roles the interserver path could not resolve.
    if (id)
    {
        const auto eit = users_external_roles.find(name);
        const bool roles_incomplete = has_role_mapping
            && ((eit == users_external_roles.end()) || (eit->second.size() != role_search_params.size()));
        /// With a service account (`lookup_bind_dn`) every cached entry is revalidated against the directory here,
        /// so `EXECUTE AS` stops resolving a user removed from the directory, or one that lost their groups, without
        /// waiting for that user's next login or a restart. Without one there is nothing to revalidate against, so
        /// only the entries the interserver path left unconfirmed (`unverified_user_names`) or with incomplete roles
        /// are re-queried; a normal login already validated the rest.
        const bool has_lookup = access_control.getExternalAuthenticators().hasLDAPLookupIdentity(ldap_server_name);
        if (has_lookup || unverified_user_names.contains(name) || roles_incomplete)
        {
            LDAPClient::SearchResultsList external_roles;
            if (!access_control.getExternalAuthenticators().findLDAPUser(
                    ldap_server_name,
                    name,
                    has_role_mapping ? &role_search_params : nullptr,
                    has_role_mapping ? &external_roles : nullptr))
            {
                LOG_DEBUG(getLogger(), "User {} is cached in LDAP directory {} but the directory does not confirm the name; not resolving it",
                    name, backQuote(getStorageName()));
                return {};
            }
            if (has_role_mapping)
                updateAssignedRolesNoLock(*id, name, external_roles);
            unverified_user_names.erase(name);
        }
        return id;
    }

    LDAPClient::SearchResultsList external_roles;
    if (!access_control.getExternalAuthenticators().findLDAPUser(
            ldap_server_name,
            name,
            has_role_mapping ? &role_search_params : nullptr,
            has_role_mapping ? &external_roles : nullptr))
    {
        return {};
    }

    /// Materialize the user with the resolved role mapping. The shape mirrors the
    /// already-tested first-login path in `authenticateImpl`, so the entry is
    /// indistinguishable from one created by a real LDAP login.
    auto new_user = makeUserNoLock(name);
    assignRolesNoLock(*new_user, external_roles);
    return memory_storage.insert(new_user);
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

    const auto & user_name = credentials.getUserName();

    /// Names listed in `exclude_users` are never served by this storage: no candidate user is built,
    /// no address check runs and the LDAP server is never contacted, for `BasicCredentials` and
    /// interserver `AlwaysAllowCredentials` alike. They are reported as "not found" rather than as
    /// an error so that `MultipleAccessStorage` continues with the storages that follow.
    if (excluded_user_names.contains(user_name))
    {
        LOG_DEBUG(getLogger(), "Skipping excluded user {}: the name is listed in exclude_users", user_name);
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, user_name, getStorageName());
        return {};
    }

    auto id = memory_storage.find<User>(user_name);

    if (sync_params)
    {
        /// Gate order is deliberate. A name outside the synchronised snapshot is "not found" (before the
        /// first successful run nobody logs in through this directory, and the storages that follow are
        /// unaffected), so the staleness gate below can only ever refuse a user this directory owns.
        if (!id && sync_params->only_synced_users)
        {
            LOG_DEBUG(getLogger(), "User {} is not in the synchronised snapshot of directory {}", user_name, backQuote(getStorageName()));
            if (throw_if_user_not_exists)
                throwNotFound(AccessEntityType::USER, user_name, getStorageName());
            return {};
        }

        if (id)
            checkNotStale(user_name, "authenticate");
    }

    UserPtr user = id ? memory_storage.read<User>(*id) : nullptr;

    std::shared_ptr<User> new_user;
    if (!user)
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        new_user = makeUserNoLock(user_name);
        user = new_user;
    }

    if (!isAddressAllowed(*user, address))
        throwAddressNotAllowed(address);

    LDAPClient::SearchResultsList external_roles;
    if (!areLDAPCredentialsValidNoLock(*user, credentials, external_authenticators, external_roles))
    {
        /// A name the synchronisation materialised is known to exist in the directory (as of the last run) and
        /// shadows the same name in the storages that follow (`planSync` says so when it materialises it): a
        /// failed bind is a wrong password or a disabled account, and the chain stops here, as it does for a
        /// local user with a wrong password. Every other name keeps the lazy semantics below.
        if (sync_params && synced_user_names.contains(user_name))
            throw Exception(ErrorCodes::WRONG_PASSWORD, "Invalid credentials");

        // We don't know why the authentication has just failed:
        // either there is no such user in LDAP or the password is not correct.
        // We treat this situation as if there is no such user because we don't want to block
        // other storages following this LDAPAccessStorage from trying to authenticate on their own.
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, user_name, getStorageName());
        else
            return {};
    }

    const bool interserver = typeid_cast<const AlwaysAllowCredentials *>(&credentials) != nullptr;

    if (new_user)
    {
        // TODO: if these were AlwaysAllowCredentials, then mapped external roles are not available here,
        // since without a password we can't authenticate and retrieve roles from the LDAP server.

        assignRolesNoLock(*new_user, external_roles);
        id = memory_storage.insert(new_user);

        /// The initiator of a distributed query vouched for the name; the directory has not been asked, and
        /// the name may not exist there at all. Remembered so that `EXECUTE AS` (`findImpl` with
        /// `force_external_lookup`) confirms the user upstream before impersonating it.
        if (interserver)
            unverified_user_names.insert(user_name);
    }
    else if (!interserver && !sync_params)
    {
        // Just in case external_roles are changed. This will be no-op if they are not.
        // Interserver `AlwaysAllowCredentials` skip the LDAP round-trip (see `areLDAPCredentialsValidNoLock`),
        // so `external_roles` is empty for them; updating from it would wipe the roles mapped at the user's
        // last password login until the next one (https://github.com/ClickHouse/ClickHouse/pull/101920).
        // In a synced directory the roles come from the synchronisation only (see `areLDAPCredentialsValidNoLock`).
        updateAssignedRolesNoLock(*id, user->getName(), external_roles);
    }

    /// A password the directory verified confirms the name, whatever materialised the entry.
    if (!interserver)
        unverified_user_names.erase(user_name);

    if (id)
        return AuthResult{ .user_id = *id, .authentication_data = AuthenticationData(AuthenticationType::LDAP), .user_name = user_name };
    return std::nullopt;
}


void LDAPAccessStorage::startPeriodicReloading()
{
    if (!sync_params)
        return;

    std::lock_guard lock(sync_thread_mutex);
    if (sync_thread)
        return;

    sync_thread_should_exit = false;
    sync_thread = std::make_unique<ThreadFromGlobalPool>(&LDAPAccessStorage::runSyncThread, this);
}


void LDAPAccessStorage::stopPeriodicReloading()
{
    stopSyncThread();
}


void LDAPAccessStorage::stopSyncThread()
{
    std::unique_ptr<ThreadFromGlobalPool> thread;
    {
        std::lock_guard lock(sync_thread_mutex);
        sync_thread_should_exit = true;
        thread = std::move(sync_thread);
    }
    sync_thread_cv.notify_all();

    /// A run in progress finishes first (its LDAP operations are bounded by the server's timeouts).
    if (thread && thread->joinable())
        thread->join();
}


void LDAPAccessStorage::shutdown()
{
    stopSyncThread();
}


void LDAPAccessStorage::reload(ReloadMode reload_mode)
{
    /// `USERS_CONFIG_ONLY` (`SYSTEM RELOAD CONFIG`) is about `users.xml`; the directory itself is configured once at startup.
    if (!sync_params || reload_mode != ReloadMode::ALL)
        return;

    /// Synchronous and propagating: `SYSTEM RELOAD USERS` reports the reason when the run refuses to apply.
    sync();
}


void LDAPAccessStorage::runSyncThread()
{
    setThreadName(ThreadName::LDAP_SYNC);

    const auto interval = sync_params->interval;

    /// Spread the first runs of several nodes started together over a tenth of the interval so that they do not
    /// all hit the directory at the same moment.
    std::uniform_int_distribution<Int64> jitter(0, interval.count() / 10);
    std::chrono::seconds wait{jitter(thread_local_rng)};

    while (true)
    {
        {
            std::unique_lock lock(sync_thread_mutex);
            if (sync_thread_cv.wait_for(lock, wait, [this] { return sync_thread_should_exit; }))
                return;
        }

        /// The only place a failed run is caught. A failure applies nothing (see `sync`), so the next run
        /// starts from the same state; it is scheduled sooner than usual to recover quickly from a transient outage.
        bool succeeded = false;
        try
        {
            sync();
            succeeded = true;
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), fmt::format("LDAP synchronisation of directory {} failed", backQuote(getStorageName())));
        }

        if (interval == std::chrono::seconds{0})
            return; /// Startup run only; `SYSTEM RELOAD USERS` triggers the next one.

        wait = succeeded ? interval : std::min(interval, std::chrono::seconds{60});
    }
}


void LDAPAccessStorage::sync()
{
    /// Runs are serialised; `mutex` is taken only for the in-memory apply phase, never around LDAP I/O.
    std::lock_guard sync_lock(sync_mutex);

    /// `ldap_server_name`, `role_search_params`, `excluded_user_names` and `sync_params` are set once by
    /// the constructor and never change, so they are read here without `mutex`.
    const auto & params = *sync_params;

    ProfileEvents::increment(ProfileEvents::LDAPSyncRuns);
    CurrentMetrics::Increment running_metric{CurrentMetrics::LDAPSyncRunning};
    Stopwatch watch;

    /// Every exit before the end of this function is a failed run, and a failed run has applied nothing:
    /// the guards trip before the first write, and the apply phase works in memory under one lock.
    bool succeeded = false;
    SCOPE_EXIT({
        if (!succeeded)
            ProfileEvents::increment(ProfileEvents::LDAPSyncFailures);
    });

    LOG_DEBUG(getLogger(), "Starting LDAP synchronisation of directory {} from server '{}'{}",
        backQuote(getStorageName()), ldap_server_name, params.dry_run ? " (dry run)" : "");

    /// Phase 1: enumerate the directory under the lookup identity. No storage lock is held meanwhile.
    auto entries = access_control.getExternalAuthenticators().enumerateLDAPUsers(ldap_server_name, params.enumeration, role_search_params);

    /// Phase 2: plan, and check the guards against the current snapshot so that nothing (roles included)
    /// is written when the run is refused. The diff is recomputed under the lock right before it is
    /// applied, because a lazy login may have materialised a user in between.
    const auto plan = planSync(std::move(entries));
    {
        std::lock_guard lock(mutex);
        checkRemovalGuard(computeSyncDiffNoLock(plan));
    }

    /// Phase 3: roles, without `mutex`: the writes go to another storage, whose notifications come back
    /// to `processRoleChange`, which takes `mutex` itself.
    std::set<String> roles_to_create;
    std::shared_ptr<IAccessStorage> roles_target;
    if (params.create_roles)
    {
        roles_target = selectRolesStorage();
        for (const auto & role_name : getAllowListedRoleNames())
        {
            if (!access_control.find<Role>(role_name))
                roles_to_create.insert(role_name);
        }
    }

    if (params.dry_run)
    {
        std::lock_guard lock(mutex);
        logDryRun(plan, computeSyncDiffNoLock(plan), roles_to_create, roles_target.get());
        succeeded = true;
        return;
    }

    size_t roles_created = 0;
    if (roles_target)
    {
        roles_created = createMissingRoles(roles_to_create, *roles_target);

        /// A role of an allow-listed name in several storages: published by one of them later, or created here
        /// before it appeared. The name resolves to the first copy in `user_directories` order and
        /// `processRoleChange` keeps the users on that one; the other copies are left to the operator, since a role
        /// can carry grants of its own.
        for (const auto & role_name : getAllowListedRoleNames())
        {
            Strings holders;
            for (const auto & storage : access_control.getStorages())
            {
                if (storage->find(AccessEntityType::ROLE, role_name))
                    holders.push_back(backQuote(storage->getStorageName()));
            }
            if (holders.size() < 2)
                continue;

            LOG_WARNING(getLogger(),
                "Role '{}' exists in storages {}; the name resolves to the copy in {}, which the users of LDAP directory {} hold; drop the other copies",
                role_name, fmt::join(holders, ", "), holders.front(), backQuote(getStorageName()));
        }
    }

    /// Phase 4: apply, in memory only, then notify the subscribers without the mutex.
    SyncApplyResult result;
    {
        std::lock_guard lock(mutex);
        const auto diff = computeSyncDiffNoLock(plan);
        checkRemovalGuard(diff);
        result = applySyncPlanNoLock(plan, diff);
        /// Stamped before the lock is released: a login that finds a user of this run must not be refused
        /// by `checkNotStale` on the age of the previous one while the subscribers are still being notified.
        last_sync_success_time_s.store(steadyNowSeconds());
    }
    /// The run is applied and accounted for from here on, whatever the notifications and the cleanup below do.
    succeeded = true;
    access_control.getChangesNotifier().sendNotifications();

    ProfileEvents::increment(ProfileEvents::LDAPSyncUsersAdded, result.added);
    ProfileEvents::increment(ProfileEvents::LDAPSyncUsersUpdated, result.updated);
    ProfileEvents::increment(ProfileEvents::LDAPSyncUsersRemoved, result.removed);
    ProfileEvents::increment(ProfileEvents::LDAPSyncRolesMissing, result.missing_roles.size());

    LOG_INFO(getLogger(),
        "LDAP synchronisation of directory {} from server '{}' finished in {} ms: {} entries, {} users ({} added, {} updated, {} removed, "
        "{} excluded, {} shadowed), {} roles created, {} roles missing",
        backQuote(getStorageName()), ldap_server_name, watch.elapsedMilliseconds(), plan.entries, plan.users.size(),
        result.added, result.updated, result.removed, plan.excluded, plan.shadowed, roles_created, result.missing_roles.size());

    if (!result.missing_roles.empty())
        LOG_WARNING(getLogger(), "Roles referenced by the role mappings of LDAP directory {} do not exist and were not granted: {}",
            backQuote(getStorageName()), fmt::join(result.missing_roles, ", "));

    /// Phase 5: a removed user may still be named by entities of other storages: `TO` lists of row policies,
    /// quotas and settings profiles, grantees and default roles of other users. `memory_storage.remove` cleaned
    /// such references inside this directory only; left in the other storages, they would point at a dead id and
    /// silently stop matching the same login when a later run materialises it again under a new id. This is what
    /// `DROP USER` does through `IAccessStorage::remove`, and it runs without `mutex` like phase 3: the writes go
    /// to other storages, whose notifications come back to this directory's subscriptions.
    if (!result.removed_ids.empty())
        access_control.dropReferencesToRemovedEntities(result.removed_ids);
}


LDAPAccessStorage::SyncPlan LDAPAccessStorage::planSync(std::vector<LDAPSyncClient::UserEntry> entries) const
{
    const auto & params = *sync_params;

    SyncPlan plan;
    plan.entries = entries.size();

    /// Two entries sharing a user name would make a login map to an arbitrary one of them; refuse the whole run.
    std::map<String, String> dn_by_name;
    for (const auto & entry : entries)
    {
        const auto [it, inserted] = dn_by_name.emplace(entry.name, entry.dn);
        if (!inserted)
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP synchronisation of directory {} from server '{}': entries '{}' and '{}' share the user name '{}' (ambiguous directory); refusing to synchronise",
                backQuote(getStorageName()), ldap_server_name, it->second, entry.dn, entry.name);
    }

    /// A storage declared before this one wins for a name it defines (the user is never materialised here);
    /// a storage declared after it is overridden by the LDAP entry, exactly as at login time. Every other storage
    /// answers from its whole user set, so its answer is authoritative: none of them is an `ldap` directory, whose
    /// answer would only cover the users it happens to hold right now (`AccessControl::checkLDAPStoragesLayout`
    /// refuses such a layout at startup).
    const auto storages = access_control.getStorages();

    for (auto & entry : entries)
    {
        if (excluded_user_names.contains(entry.name))
        {
            ++plan.excluded;
            LOG_DEBUG(getLogger(), "Skipping excluded user {}: the name is listed in exclude_users", entry.name);
            continue;
        }

        bool shadowed_by_preceding = false;
        bool before_this = true;
        for (const auto & storage : storages)
        {
            if (storage.get() == this)
            {
                before_this = false;
                continue;
            }

            if (!storage->find<User>(entry.name))
                continue;

            ++plan.shadowed;
            if (before_this)
            {
                shadowed_by_preceding = true;
                LOG_WARNING(getLogger(), "LDAP user '{}' ({}) exists in storage {}, which precedes directory {}: not synchronised",
                    entry.name, entry.dn, backQuote(storage->getStorageName()), backQuote(getStorageName()));
            }
            else
            {
                LOG_WARNING(getLogger(), "LDAP user '{}' ({}) also exists in storage {}, which follows directory {}: the LDAP entry takes precedence",
                    entry.name, entry.dn, backQuote(storage->getStorageName()), backQuote(getStorageName()));
            }
            break;
        }

        if (shadowed_by_preceding)
            continue;

        plan.users.emplace(entry.name, std::move(entry));
    }

    ProfileEvents::increment(ProfileEvents::LDAPSyncUsersExcluded, plan.excluded);
    ProfileEvents::increment(ProfileEvents::LDAPSyncUsersShadowed, plan.shadowed);

    if (plan.users.size() < params.min_users)
        throw Exception(ErrorCodes::LDAP_ERROR,
            "LDAP synchronisation of directory {} from server '{}' found {} users in {} entries, fewer than min_users = {}; refusing to synchronise",
            backQuote(getStorageName()), ldap_server_name, plan.users.size(), plan.entries, params.min_users);

    return plan;
}


LDAPAccessStorage::SyncDiff LDAPAccessStorage::computeSyncDiffNoLock(const SyncPlan & plan) const
{
    SyncDiff diff;
    diff.synced = synced_user_names.size();

    /// `planSync` decided which names a preceding storage shadows, but it ran without the lock and before this diff.
    /// A `CREATE USER` or a reload of an earlier storage may have defined one of the planned names since, so the
    /// decision is re-checked here, under the lock right before the plan is applied: this run then never materialises
    /// a name a preceding storage now owns, which would leave the login resolving to that storage while a second
    /// copy of the user lingers in this directory until the next run.
    const auto storages = access_control.getStorages();
    auto shadowed_by_preceding = [&](const String & name)
    {
        for (const auto & storage : storages)
        {
            if (storage.get() == this)
                return false;
            if (storage->find<User>(name))
                return true;
        }
        return false;
    };

    for (const auto & [name, entry] : plan.users)
    {
        if (shadowed_by_preceding(name))
            continue;

        const auto id = memory_storage.find<User>(name);
        if (!id)
        {
            diff.to_add.push_back(&entry);
            continue;
        }

        /// `updateAssignedRolesNoLock` is a no-op for an unchanged set; only real changes are counted.
        const auto it = users_external_roles.find(name);
        if (it == users_external_roles.end() || it->second != entry.external_roles)
            diff.to_update.emplace_back(*id, &entry);
    }

    /// Only users the synchronisation materialised are its to remove; a user that logged in lazily
    /// (possible without `only_synced_users`) keeps the lifetime it has always had.
    for (const auto & name : synced_user_names)
    {
        if (plan.users.contains(name))
            continue;

        if (const auto id = memory_storage.find<User>(name))
            diff.to_remove.emplace_back(*id, name);
    }

    return diff;
}


void LDAPAccessStorage::checkRemovalGuard(const SyncDiff & diff) const
{
    const auto & params = *sync_params;

    /// At least one removal is always allowed, otherwise a directory with a single user could never offboard them.
    const size_t limit = std::max<size_t>(1, static_cast<size_t>(std::floor(params.max_removed_fraction * static_cast<double>(diff.synced))));
    if (diff.to_remove.size() > limit)
        throw Exception(ErrorCodes::LDAP_ERROR,
            "LDAP synchronisation of directory {} from server '{}' would remove {} of {} users, more than max_removed_fraction = {} allows ({}); refusing to synchronise",
            backQuote(getStorageName()), ldap_server_name, diff.to_remove.size(), diff.synced, params.max_removed_fraction, limit);
}


std::set<String> LDAPAccessStorage::getAllowListedRoleNames() const
{
    /// The role name of an allow-listed group is its configured spelling (plain form) or the `rdn_attribute`
    /// value of the configured DN (DN form) with `prefix` removed; `parseLDAPRoleSearchParams` guarantees
    /// that every entry starts with the prefix and is longer than it.
    std::set<String> role_names;
    for (const auto & mapping : role_search_params)
    {
        for (const auto & [_, configured_name] : mapping.plain_groups)
            role_names.emplace(configured_name, mapping.prefix.size());
        for (const auto & [_, rdn_value] : mapping.dn_groups)
            role_names.emplace(rdn_value, mapping.prefix.size());
    }
    return role_names;
}


std::shared_ptr<IAccessStorage> LDAPAccessStorage::selectRolesStorage()
{
    const auto & params = *sync_params;
    std::shared_ptr<IAccessStorage> storage;

    if (!params.roles_storage.empty())
    {
        storage = access_control.getStorageByName(params.roles_storage);
        if (storage->isReadOnly())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "LDAP sync cannot create roles in read-only storage {}; set roles_storage to a writable storage", backQuote(storage->getStorageName()));
    }
    else
    {
        std::vector<String> writable_names;
        for (const auto & candidate : access_control.getStorages())
        {
            if (candidate->isReadOnly())
                continue;
            if (!storage)
                storage = candidate;
            writable_names.push_back(candidate->getStorageName());
        }

        if (!storage)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "LDAP sync of directory {} cannot create roles: there is no writable access storage", backQuote(getStorageName()));

        if (writable_names.size() > 1 && !roles_storage_pick_logged)
        {
            roles_storage_pick_logged = true;
            LOG_WARNING(getLogger(), "LDAP sync of directory {} creates roles in storage {}, the first writable one of [{}]; set roles_storage to choose explicitly",
                backQuote(getStorageName()), backQuote(storage->getStorageName()), fmt::join(writable_names, ", "));
        }
    }

    /// `MultipleAccessStorage::insertImpl` would happily pick a `memory` directory declared before the
    /// persistent one; roles that vanish at restart would take the DBA's grants on them along.
    if (storage->isEphemeral())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LDAP sync cannot create roles in ephemeral storage {}; set roles_storage", backQuote(storage->getStorageName()));

    return storage;
}


size_t LDAPAccessStorage::createMissingRoles(const std::set<String> & role_names, IAccessStorage & storage)
{
    /// The name of a storage other than `storage` that holds a role of this name, if any.
    auto find_elsewhere = [&](const String & role_name) -> std::optional<String>
    {
        for (const auto & other : access_control.getStorages())
        {
            if (other.get() != &storage && other->find(AccessEntityType::ROLE, role_name))
                return other->getStorageName();
        }
        return {};
    };

    size_t created = 0;
    size_t removed = 0;
    for (const auto & role_name : role_names)
    {
        /// The guard `CREATE ROLE` gets from `MultipleAccessStorage::insertImpl`: the role must not exist in any
        /// storage, not only in the one written to. Checked right before the write, and again right after it,
        /// because another session can create the role through `AccessControl` in between; the name would then
        /// resolve to whichever storage comes first in `user_directories` while the other copy lingers. The copy
        /// this run wrote has no grants yet, so it is the one to give up.
        if (const auto elsewhere = find_elsewhere(role_name))
        {
            LOG_DEBUG(getLogger(), "Role '{}' exists in storage {}; LDAP directory {} does not create it",
                role_name, backQuote(*elsewhere), backQuote(getStorageName()));
            continue;
        }

        auto role = std::make_shared<Role>();
        role->setName(role_name);

        /// `CREATE ROLE IF NOT EXISTS` semantics: a node that lost the race against another node gets nullopt.
        const auto id = storage.tryInsert(role);
        if (!id)
            continue;

        if (const auto elsewhere = find_elsewhere(role_name))
        {
            /// The copy was resolvable by name for a moment, so another session may already have granted it or
            /// named it in a `TO` list. Removed the way `DROP ROLE` removes, through `AccessControl`, whose
            /// `removeReferencesToRemovedIDs` cleans such references in every storage, not only in `storage`.
            access_control.tryRemove(*id);
            ++removed;
            LOG_WARNING(getLogger(), "Role '{}' was created in storage {} while LDAP directory {} was creating it in storage {}; the latter copy was removed",
                role_name, backQuote(*elsewhere), backQuote(getStorageName()), backQuote(storage.getStorageName()));
            continue;
        }

        ++created;
        LOG_INFO(getLogger(), "Created role '{}' in storage {} for LDAP directory {}", role_name, backQuote(storage.getStorageName()), backQuote(getStorageName()));
    }

    if (created > 0)
        ProfileEvents::increment(ProfileEvents::LDAPSyncRolesCreated, created);

    /// Direct storage writes only enqueue notifications; deliver them so that `processRoleChange`
    /// learns the ids (and forgets the removed ones) before the users are assigned.
    if (created > 0 || removed > 0)
        access_control.getChangesNotifier().sendNotifications();

    return created;
}


LDAPAccessStorage::SyncApplyResult LDAPAccessStorage::applySyncPlanNoLock(const SyncPlan & plan, const SyncDiff & diff)
{
    SyncApplyResult result;

    for (const auto & [id, name] : diff.to_remove)
    {
        memory_storage.remove(id);
        removeUserNoLock(name);
        result.removed_ids.insert(id);
        LOG_INFO(getLogger(), "Removed LDAP user '{}' from directory {}: no longer returned by the directory", name, backQuote(getStorageName()));
    }
    result.removed = diff.to_remove.size();

    for (const auto & [id, entry] : diff.to_update)
    {
        updateAssignedRolesNoLock(id, entry->name, entry->external_roles);
        LOG_INFO(getLogger(), "Updated roles of LDAP user '{}' ({}) in directory {}", entry->name, entry->dn, backQuote(getStorageName()));
    }
    result.updated = diff.to_update.size();

    for (const auto * entry : diff.to_add)
    {
        auto new_user = makeUserNoLock(entry->name);
        assignRolesNoLock(*new_user, entry->external_roles);
        memory_storage.insert(new_user);
        LOG_INFO(getLogger(), "Added LDAP user '{}' ({}) to directory {}", entry->name, entry->dn, backQuote(getStorageName()));
    }
    result.added = diff.to_add.size();

    /// From now on every planned user belongs to the synchronisation, including the ones that had logged in lazily
    /// or were materialised by the interserver path: the directory has just confirmed each of them, so `EXECUTE AS`
    /// resolves them from the snapshot like every synchronised user, without a lookup.
    synced_user_names.clear();
    for (const auto & [name, _] : plan.users)
    {
        synced_user_names.insert(name);
        unverified_user_names.erase(name);
    }

    /// Roles the mappings name but nobody created: `assignRolesNoLock` warned per user, this is the per-run view.
    for (const auto & [role_name, _] : users_per_roles)
    {
        if (!granted_role_ids.contains(role_name))
            result.missing_roles.insert(role_name);
    }
    for (const auto & role_name : common_role_names)
    {
        if (!granted_role_ids.contains(role_name))
            result.missing_roles.insert(role_name);
    }

    return result;
}


void LDAPAccessStorage::logDryRun(const SyncPlan & plan, const SyncDiff & diff, const std::set<String> & roles_to_create, const IAccessStorage * roles_target) const
{
    for (const auto * entry : diff.to_add)
        LOG_INFO(getLogger(), "Dry run: would add LDAP user '{}' ({}) to directory {}", entry->name, entry->dn, backQuote(getStorageName()));

    for (const auto & [id, entry] : diff.to_update)
        LOG_INFO(getLogger(), "Dry run: would update roles of LDAP user '{}' ({}) in directory {}", entry->name, entry->dn, backQuote(getStorageName()));

    for (const auto & [id, name] : diff.to_remove)
        LOG_INFO(getLogger(), "Dry run: would remove LDAP user '{}' from directory {}", name, backQuote(getStorageName()));

    for (const auto & role_name : roles_to_create)
        LOG_INFO(getLogger(), "Dry run: would create role '{}' in storage {}", role_name, backQuote(roles_target->getStorageName()));

    LOG_INFO(getLogger(),
        "LDAP synchronisation of directory {} from server '{}' (dry run) finished: {} entries, {} users (would add {}, update {}, remove {}; "
        "{} excluded, {} shadowed), would create {} roles; nothing applied",
        backQuote(getStorageName()), ldap_server_name, plan.entries, plan.users.size(),
        diff.to_add.size(), diff.to_update.size(), diff.to_remove.size(), plan.excluded, plan.shadowed, roles_to_create.size());
}

}
