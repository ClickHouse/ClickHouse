#include <Access/LDAPAccessStorage.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/container_hash/hash.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <iterator>
#include <regex>
#include <sstream>
#include <unordered_map>


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
    const bool has_role_mapping = config.has(prefix_str + "role_mapping");

    if (!has_server)
        throw Exception("Missing 'server' field for LDAP user directory", ErrorCodes::BAD_ARGUMENTS);

    const auto ldap_server_cfg = config.getString(prefix_str + "server");
    if (ldap_server_cfg.empty())
        throw Exception("Empty 'server' field for LDAP user directory", ErrorCodes::BAD_ARGUMENTS);

    std::set<String> common_roles_cfg;
    if (has_roles)
    {
        Poco::Util::AbstractConfiguration::Keys role_names;
        config.keys(prefix_str + "roles", role_names);

        // Currently, we only extract names of roles from the section names and assign them directly and unconditionally.
        common_roles_cfg.insert(role_names.begin(), role_names.end());
    }

    LDAPSearchParamsList role_search_params_cfg;
    if (has_role_mapping)
    {
        Poco::Util::AbstractConfiguration::Keys all_keys;
        config.keys(prefix, all_keys);
        for (const auto & key : all_keys)
        {
            if (key != "role_mapping" && key.find("role_mapping[") != 0)
                continue;

            const String rm_prefix = prefix_str + key;
            const String rm_prefix_str = rm_prefix + '.';
            role_search_params_cfg.emplace_back();
            auto & rm_params = role_search_params_cfg.back();

            rm_params.base_dn = config.getString(rm_prefix_str + "base_dn", "");
            rm_params.search_filter = config.getString(rm_prefix_str + "search_filter", "");
            rm_params.attribute = config.getString(rm_prefix_str + "attribute", "cn");
            rm_params.fail_if_all_rules_mismatch = config.getBool(rm_prefix_str + "fail_if_all_rules_mismatch", true);

            auto scope = config.getString(rm_prefix_str + "scope", "subtree");
            boost::algorithm::to_lower(scope);
            if (scope == "base")           rm_params.scope = LDAPSearchParams::Scope::BASE;
            else if (scope == "one_level") rm_params.scope = LDAPSearchParams::Scope::ONE_LEVEL;
            else if (scope == "subtree")   rm_params.scope = LDAPSearchParams::Scope::SUBTREE;
            else if (scope == "children")  rm_params.scope = LDAPSearchParams::Scope::CHILDREN;
            else
                throw Exception("Invalid value of 'scope' field in '" + key + "' section of LDAP user directory, must be one of 'base', 'one_level', 'subtree', or 'children'", ErrorCodes::BAD_ARGUMENTS);

            Poco::Util::AbstractConfiguration::Keys all_mapping_keys;
            config.keys(rm_prefix, all_mapping_keys);
            for (const auto & mkey : all_mapping_keys)
            {
                if (mkey != "rule" && mkey.find("rule[") != 0)
                    continue;

                const String rule_prefix = rm_prefix_str + mkey;
                const String rule_prefix_str = rule_prefix + '.';
                rm_params.rules.emplace_back();
                auto & rule = rm_params.rules.back();

                rule.match = config.getString(rule_prefix_str + "match", ".+");
                try
                {
                    // Construct unused regex instance just to check the syntax.
                    std::regex(rule.match, std::regex_constants::ECMAScript);
                }
                catch (const std::regex_error & e)
                {
                    throw Exception("ECMAScript regex syntax error in 'match' field in '" + mkey + "' rule of '" + key + "' section of LDAP user directory: " + e.what(), ErrorCodes::BAD_ARGUMENTS);
                }

                rule.replace = config.getString(rule_prefix_str + "replace", "$&");
                rule.continue_on_match = config.getBool(rule_prefix_str + "continue_on_match", false);
            }
        }
    }

    access_control_manager = access_control_manager_;
    ldap_server = ldap_server_cfg;
    role_search_params.swap(role_search_params_cfg);
    common_role_names.swap(common_roles_cfg);

    users_per_roles.clear();
    granted_role_names.clear();
    granted_role_ids.clear();
    external_role_hashes.clear();

    role_change_subscription = access_control_manager->subscribeForChanges<Role>(
        [this] (const UUID & id, const AccessEntityPtr & entity)
        {
            return this->processRoleChange(id, entity);
        }
    );

    // Update granted_role_* with the initial values: resolved ids of roles from common_role_names.
    for (const auto & role_name : common_role_names)
    {
        if (const auto role_id = access_control_manager->find<Role>(role_name))
        {
            granted_role_names.insert_or_assign(*role_id, role_name);
            granted_role_ids.insert_or_assign(role_name, *role_id);
        }
    }
}


void LDAPAccessStorage::processRoleChange(const UUID & id, const AccessEntityPtr & entity)
{
    std::scoped_lock lock(mutex);
    auto role = typeid_cast<std::shared_ptr<const Role>>(entity);
    const auto it = granted_role_names.find(id);

    if (role) // Added or renamed role.
    {
        const auto & new_role_name = role->getName();
        if (it != granted_role_names.end())
        {
            // Revoke the old role if its name has been changed.
            const auto & old_role_name = it->second;
            if (new_role_name != old_role_name)
            {
                applyRoleChangeNoLock(false /* revoke */, id, old_role_name);
            }
        }

        // Grant the role.
        applyRoleChangeNoLock(true /* grant */, id, new_role_name);
    }
    else // Removed role.
    {
        if (it != granted_role_names.end())
        {
            // Revoke the old role.
            const auto & old_role_name = it->second;
            applyRoleChangeNoLock(false /* revoke */, id, old_role_name);
        }
    }
}


void LDAPAccessStorage::applyRoleChangeNoLock(bool grant, const UUID & role_id, const String & role_name)
{
    std::vector<UUID> user_ids;

    // Find relevant user ids.
    if (common_role_names.count(role_name))
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

    // Update relevant users' granted roles.
    if (!user_ids.empty())
    {
        auto update_func = [&role_id, &grant] (const AccessEntityPtr & entity_) -> AccessEntityPtr
        {
            if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
            {
                auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
                auto & granted_roles = changed_user->granted_roles.roles;

                if (grant)
                    granted_roles.insert(role_id);
                else
                    granted_roles.erase(role_id);

                return changed_user;
            }
            return entity_;
        };

        memory_storage.update(user_ids, update_func);

        if (grant)
        {
            granted_role_names.insert_or_assign(role_id, role_name);
            granted_role_ids.insert_or_assign(role_name, role_id);
        }
        else
        {
            granted_role_names.erase(role_id);
            granted_role_ids.erase(role_name);
        }
    }
}


void LDAPAccessStorage::grantRolesNoLock(User & user, const LDAPSearchResultsList & external_roles) const
{
    const auto & user_name = user.getName();
    const auto new_hash = boost::hash<LDAPSearchResultsList>{}(external_roles);
    auto & granted_roles = user.granted_roles.roles;

    // Map external role names to local role names.
    const auto user_role_names = mapExternalRolesNoLock(user_name, external_roles);

    external_role_hashes.erase(user_name);
    granted_roles.clear();

    // Grant the common roles.

    // Initially, all the available ids of common roles were resolved in setConfiguration(),
    // and, then, maintained by processRoleChange(), so here we just grant those that exist (i.e., resolved).
    for (const auto & role_name : common_role_names)
    {
        const auto it = granted_role_ids.find(role_name);
        if (it == granted_role_ids.end())
        {
            LOG_WARNING(getLogger(), "Unable to grant common role '{}' to user '{}': role not found", role_name, user_name);
        }
        else
        {
            const auto & role_id = it->second;
            granted_roles.insert(role_id);
        }
    }

    // Grant the mapped external roles.

    // Cleanup helper relations.
    for (auto it = users_per_roles.begin(); it != users_per_roles.end();)
    {
        const auto & role_name = it->first;
        auto & user_names = it->second;
        if (user_role_names.count(role_name) == 0)
        {
            user_names.erase(user_name);
            if (user_names.empty())
            {
                if (common_role_names.count(role_name) == 0)
                {
                    auto rit = granted_role_ids.find(role_name);
                    if (rit != granted_role_ids.end())
                    {
                        granted_role_names.erase(rit->second);
                        granted_role_ids.erase(rit);
                    }
                }
                users_per_roles.erase(it++);
            }
            else
            {
                ++it;
            }
        }
        else
        {
            ++it;
        }
    }

    // Resolve and assign mapped external role ids.
    for (const auto & role_name : user_role_names)
    {
        users_per_roles[role_name].insert(user_name);
        const auto it = granted_role_ids.find(role_name);
        if (it == granted_role_ids.end())
        {
            if (const auto role_id = access_control_manager->find<Role>(role_name))
            {
                granted_roles.insert(*role_id);
                granted_role_names.insert_or_assign(*role_id, role_name);
                granted_role_ids.insert_or_assign(role_name, *role_id);
            }
            else
            {
                LOG_WARNING(getLogger(), "Unable to grant mapped role '{}' to user '{}': role not found", role_name, user_name);
            }
        }
        else
        {
            const auto & role_id = it->second;
            granted_roles.insert(role_id);
        }
    }

    external_role_hashes[user_name] = new_hash;
}


void LDAPAccessStorage::updateRolesNoLock(const UUID & id, const String & user_name, const LDAPSearchResultsList & external_roles) const
{
    // common_role_names are not included since they don't change.
    const auto new_hash = boost::hash<LDAPSearchResultsList>{}(external_roles);

    const auto it = external_role_hashes.find(user_name);
    if (it != external_role_hashes.end() && it->second == new_hash)
        return;

    auto update_func = [this, &external_roles] (const AccessEntityPtr & entity_) -> AccessEntityPtr
    {
        if (auto user = typeid_cast<std::shared_ptr<const User>>(entity_))
        {
            auto changed_user = typeid_cast<std::shared_ptr<User>>(user->clone());
            grantRolesNoLock(*changed_user, external_roles);
            return changed_user;
        }
        return entity_;
    };

    memory_storage.update(id, update_func);
}


std::set<String> LDAPAccessStorage::mapExternalRolesNoLock(const String & user_name, const LDAPSearchResultsList & external_roles) const
{
    std::set<String> role_names;

    if (external_roles.size() != role_search_params.size())
        throw Exception("Unable to match external roles to mapping rules", ErrorCodes::BAD_ARGUMENTS);

    std::unordered_map<String, std::regex> re_cache;
    for (std::size_t i = 0; i < external_roles.size(); ++i)
    {
        const auto & external_role_set = external_roles[i];
        const auto & rules = role_search_params[i].rules;
        for (const auto & external_role : external_role_set)
        {
            bool have_match = false;
            for (const auto & rule : rules)
            {
                const auto & re = re_cache.try_emplace(rule.match, rule.match, std::regex_constants::ECMAScript | std::regex_constants::optimize).first->second;
                std::smatch match_results;
                if (std::regex_match(external_role, match_results, re))
                {
                    role_names.emplace(match_results.format(rule.replace));
                    have_match = true;
                    if (!rule.continue_on_match)
                        break;
                }
            }

            if (!have_match && role_search_params[i].fail_if_all_rules_mismatch)
                throw Exception("None of the external role mapping rules were able to match '" + external_role + "' string, received from LDAP server '" + ldap_server + "' for user '" + user_name + "'", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    return role_names;
}


bool LDAPAccessStorage::isPasswordCorrectLDAPNoLock(const User & user, const String & password, const ExternalAuthenticators & external_authenticators, LDAPSearchResultsList & search_results) const
{
    return user.authentication.isCorrectPasswordLDAP(password, user.getName(), external_authenticators, &role_search_params, &search_results);
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
        role_mapping_json.set("fail_if_all_rules_mismatch", role_mapping.fail_if_all_rules_mismatch);

        String scope;
        switch (role_mapping.scope)
        {
            case LDAPSearchParams::Scope::BASE:      scope = "base"; break;
            case LDAPSearchParams::Scope::ONE_LEVEL: scope = "one_level"; break;
            case LDAPSearchParams::Scope::SUBTREE:   scope = "subtree"; break;
            case LDAPSearchParams::Scope::CHILDREN:  scope = "children"; break;
        }
        role_mapping_json.set("scope", scope);

        Poco::JSON::Array rules_json;
        for (const auto & rule : role_mapping.rules)
        {
            Poco::JSON::Object rule_json;
            rule_json.set("match", rule.match);
            rule_json.set("replace", rule.replace);
            rule_json.set("continue_on_match", rule.continue_on_match);
            rules_json.add(rule_json);
        }
        role_mapping_json.set("rules", rules_json);

        role_mappings_json.add(role_mapping_json);
    }
    params_json.set("role_mappings", role_mappings_json);

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
    LDAPSearchResultsList external_roles;
    auto id = memory_storage.find<User>(user_name);
    if (id)
    {
        auto user = memory_storage.read<User>(*id);

        if (!isPasswordCorrectLDAPNoLock(*user, password, external_authenticators, external_roles))
            throwInvalidPassword();

        if (!isAddressAllowedImpl(*user, address))
            throwAddressNotAllowed(address);

        // Just in case external_roles are changed. This will be no-op if they are not.
        updateRolesNoLock(*id, user_name, external_roles);

        return *id;
    }
    else
    {
        // User does not exist, so we create one, and will add it if authentication is successful.
        auto user = std::make_shared<User>();
        user->setName(user_name);
        user->authentication = Authentication(Authentication::Type::LDAP_SERVER);
        user->authentication.setServerName(ldap_server);

        if (!isPasswordCorrectLDAPNoLock(*user, password, external_authenticators, external_roles))
            throwInvalidPassword();

        if (!isAddressAllowedImpl(*user, address))
            throwAddressNotAllowed(address);

        grantRolesNoLock(*user, external_roles);

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

        LDAPSearchResultsList external_roles;
        // TODO: mapped external roles are not available here. Implement?

        grantRolesNoLock(*user, external_roles);

        return memory_storage.insert(user);
    }
}

}
