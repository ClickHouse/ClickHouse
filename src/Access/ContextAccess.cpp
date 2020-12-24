#include <Access/ContextAccess.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledRoles.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledSettings.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <assert.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int UNKNOWN_USER;
}


namespace
{
    AccessRights mixAccessRightsFromUserAndRoles(const User & user, const EnabledRolesInfo & roles_info)
    {
        AccessRights res = user.access;
        res.makeUnion(roles_info.access);
        return res;
    }


    void applyParamsToAccessRights(AccessRights & access, const ContextAccessParams & params)
    {
        static const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
            | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
            | AccessType::TRUNCATE;

        static const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY;
        static const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
        static const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;
        static const AccessFlags write_dcl_access = AccessType::ACCESS_MANAGEMENT - AccessType::SHOW_ACCESS;

        if (params.readonly)
            access.revoke(write_table_access | table_and_dictionary_ddl | write_dcl_access | AccessType::SYSTEM | AccessType::KILL_QUERY);

        if (params.readonly == 1)
        {
            /// Table functions are forbidden in readonly mode.
            /// For readonly = 2 they're allowed.
            access.revoke(AccessType::CREATE_TEMPORARY_TABLE);
        }

        if (!params.allow_ddl)
            access.revoke(table_and_dictionary_ddl);

        if (!params.allow_introspection)
            access.revoke(AccessType::INTROSPECTION);

        if (params.readonly)
        {
            /// No grant option in readonly mode.
            access.revokeGrantOption(AccessType::ALL);
        }
    }


    void addImplicitAccessRights(AccessRights & access)
    {
        auto modifier = [&](const AccessFlags & flags, const AccessFlags & min_flags_with_children, const AccessFlags & max_flags_with_children, const std::string_view & database, const std::string_view & table, const std::string_view & column) -> AccessFlags
        {
            size_t level = !database.empty() + !table.empty() + !column.empty();
            AccessFlags res = flags;

            /// CREATE_TABLE => CREATE_VIEW, DROP_TABLE => DROP_VIEW, ALTER_TABLE => ALTER_VIEW
            static const AccessFlags create_table = AccessType::CREATE_TABLE;
            static const AccessFlags create_view = AccessType::CREATE_VIEW;
            static const AccessFlags drop_table = AccessType::DROP_TABLE;
            static const AccessFlags drop_view = AccessType::DROP_VIEW;
            static const AccessFlags alter_table = AccessType::ALTER_TABLE;
            static const AccessFlags alter_view = AccessType::ALTER_VIEW;

            if (res & create_table)
                res |= create_view;

            if (res & drop_table)
                res |= drop_view;

            if (res & alter_table)
                res |= alter_view;

            /// CREATE TABLE (on any database/table) => CREATE_TEMPORARY_TABLE (global)
            static const AccessFlags create_temporary_table = AccessType::CREATE_TEMPORARY_TABLE;
            if ((level == 0) && (max_flags_with_children & create_table))
                res |= create_temporary_table;

            /// ALTER_TTL => ALTER_MATERIALIZE_TTL
            static const AccessFlags alter_ttl = AccessType::ALTER_TTL;
            static const AccessFlags alter_materialize_ttl = AccessType::ALTER_MATERIALIZE_TTL;
            if (res & alter_ttl)
                res |= alter_materialize_ttl;

            /// RELOAD_DICTIONARY (global) => RELOAD_EMBEDDED_DICTIONARIES (global)
            static const AccessFlags reload_dictionary = AccessType::SYSTEM_RELOAD_DICTIONARY;
            static const AccessFlags reload_embedded_dictionaries = AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES;
            if ((level == 0) && (min_flags_with_children & reload_dictionary))
                res |= reload_embedded_dictionaries;

            /// any column flag => SHOW_COLUMNS => SHOW_TABLES => SHOW_DATABASES
            ///                  any table flag => SHOW_TABLES => SHOW_DATABASES
            ///       any dictionary flag => SHOW_DICTIONARIES => SHOW_DATABASES
            ///                              any database flag => SHOW_DATABASES
            static const AccessFlags show_columns = AccessType::SHOW_COLUMNS;
            static const AccessFlags show_tables = AccessType::SHOW_TABLES;
            static const AccessFlags show_dictionaries = AccessType::SHOW_DICTIONARIES;
            static const AccessFlags show_tables_or_dictionaries = show_tables | show_dictionaries;
            static const AccessFlags show_databases = AccessType::SHOW_DATABASES;

            if (res & AccessFlags::allColumnFlags())
                res |= show_columns;

            if ((res & AccessFlags::allTableFlags())
                || (level <= 2 && (res & show_columns))
                || (level == 2 && (max_flags_with_children & show_columns)))
            {
                res |= show_tables;
            }

            if (res & AccessFlags::allDictionaryFlags())
                res |= show_dictionaries;

            if ((res & AccessFlags::allDatabaseFlags())
                || (level <= 1 && (res & show_tables_or_dictionaries))
                || (level == 1 && (max_flags_with_children & show_tables_or_dictionaries)))
            {
                res |= show_databases;
            }

            return res;
        };

        access.modifyFlags(modifier);

        /// Transform access to temporary tables into access to "_temporary_and_external_tables" database.
        if (access.isGranted(AccessType::CREATE_TEMPORARY_TABLE))
            access.grant(AccessFlags::allTableFlags() | AccessFlags::allColumnFlags(), DatabaseCatalog::TEMPORARY_DATABASE);

        /// Anyone has access to the "system" database.
        access.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE);
    }


    AccessRights calculateFinalAccessRights(const AccessRights & access_from_user_and_roles, const ContextAccessParams & params)
    {
        AccessRights res_access = access_from_user_and_roles;
        applyParamsToAccessRights(res_access, params);
        addImplicitAccessRights(res_access);
        return res_access;
    }


    std::array<UUID, 1> to_array(const UUID & id)
    {
        std::array<UUID, 1> ids;
        ids[0] = id;
        return ids;
    }
}


ContextAccess::ContextAccess(const AccessControlManager & manager_, const Params & params_)
    : manager(&manager_)
    , params(params_)
{
    subscription_for_user_change = manager->subscribeForChanges(
        *params.user_id, [this](const UUID &, const AccessEntityPtr & entity)
    {
        UserPtr changed_user = entity ? typeid_cast<UserPtr>(entity) : nullptr;
        std::lock_guard lock{mutex};
        setUser(changed_user);
    });

    setUser(manager->read<User>(*params.user_id));
}


void ContextAccess::setUser(const UserPtr & user_) const
{
    user = user_;
    if (!user)
    {
        /// User has been dropped.
        auto nothing_granted = std::make_shared<AccessRights>();
        access = nothing_granted;
        access_without_readonly = nothing_granted;
        access_with_allow_ddl = nothing_granted;
        access_with_allow_introspection = nothing_granted;
        access_from_user_and_roles = nothing_granted;
        subscription_for_user_change = {};
        subscription_for_roles_changes = {};
        enabled_roles = nullptr;
        roles_info = nullptr;
        enabled_row_policies = nullptr;
        enabled_quota = nullptr;
        enabled_settings = nullptr;
        return;
    }

    user_name = user->getName();
    trace_log = &Poco::Logger::get("ContextAccess (" + user_name + ")");

    boost::container::flat_set<UUID> current_roles, current_roles_with_admin_option;
    if (params.use_default_roles)
    {
        for (const UUID & id : user->granted_roles.roles)
        {
            if (user->default_roles.match(id))
                current_roles.emplace(id);
        }
    }
    else
    {
        boost::range::set_intersection(
            params.current_roles,
            user->granted_roles.roles,
            std::inserter(current_roles, current_roles.end()));
    }

    boost::range::set_intersection(
        current_roles,
        user->granted_roles.roles_with_admin_option,
        std::inserter(current_roles_with_admin_option, current_roles_with_admin_option.end()));

    subscription_for_roles_changes = {};
    enabled_roles = manager->getEnabledRoles(current_roles, current_roles_with_admin_option);
    subscription_for_roles_changes = enabled_roles->subscribeForChanges([this](const std::shared_ptr<const EnabledRolesInfo> & roles_info_)
    {
        std::lock_guard lock{mutex};
        setRolesInfo(roles_info_);
    });

    setRolesInfo(enabled_roles->getRolesInfo());
}


void ContextAccess::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const
{
    assert(roles_info_);
    roles_info = roles_info_;
    enabled_row_policies = manager->getEnabledRowPolicies(*params.user_id, roles_info->enabled_roles);
    enabled_quota = manager->getEnabledQuota(*params.user_id, user_name, roles_info->enabled_roles, params.address, params.quota_key);
    enabled_settings = manager->getEnabledSettings(*params.user_id, user->settings, roles_info->enabled_roles, roles_info->settings_from_enabled_roles);
    calculateAccessRights();
}


void ContextAccess::calculateAccessRights() const
{
    access_from_user_and_roles = std::make_shared<AccessRights>(mixAccessRightsFromUserAndRoles(*user, *roles_info));
    access = std::make_shared<AccessRights>(calculateFinalAccessRights(*access_from_user_and_roles, params));

    access_without_readonly = nullptr;
    access_with_allow_ddl = nullptr;
    access_with_allow_introspection = nullptr;

    if (trace_log)
    {
        if (roles_info && !roles_info->getCurrentRolesNames().empty())
        {
            LOG_TRACE(trace_log, "Current_roles: {}, enabled_roles: {}",
                boost::algorithm::join(roles_info->getCurrentRolesNames(), ", "),
                boost::algorithm::join(roles_info->getEnabledRolesNames(), ", "));
        }
        LOG_TRACE(trace_log, "Settings: readonly={}, allow_ddl={}, allow_introspection_functions={}", params.readonly, params.allow_ddl, params.allow_introspection);
        LOG_TRACE(trace_log, "List of all grants: {}", access->toString());
    }
}


UserPtr ContextAccess::getUser() const
{
    std::lock_guard lock{mutex};
    return user;
}

String ContextAccess::getUserName() const
{
    std::lock_guard lock{mutex};
    return user_name;
}

std::shared_ptr<const EnabledRolesInfo> ContextAccess::getRolesInfo() const
{
    std::lock_guard lock{mutex};
    return roles_info;
}

std::shared_ptr<const EnabledRowPolicies> ContextAccess::getEnabledRowPolicies() const
{
    std::lock_guard lock{mutex};
    return enabled_row_policies;
}

ASTPtr ContextAccess::getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType index, const ASTPtr & extra_condition) const
{
    std::lock_guard lock{mutex};
    return enabled_row_policies ? enabled_row_policies->getCondition(database, table_name, index, extra_condition) : nullptr;
}

std::shared_ptr<const EnabledQuota> ContextAccess::getQuota() const
{
    std::lock_guard lock{mutex};
    return enabled_quota;
}


std::optional<QuotaUsage> ContextAccess::getQuotaUsage() const
{
    std::lock_guard lock{mutex};
    return enabled_quota ? enabled_quota->getUsage() : std::optional<QuotaUsage>{};
}


std::shared_ptr<const ContextAccess> ContextAccess::getFullAccess()
{
    static const std::shared_ptr<const ContextAccess> res = []
    {
        auto full_access = std::shared_ptr<ContextAccess>(new ContextAccess);
        full_access->access = std::make_shared<AccessRights>(AccessRights::getFullAccess());
        full_access->enabled_quota = EnabledQuota::getUnlimitedQuota();
        return full_access;
    }();
    return res;
}


std::shared_ptr<const Settings> ContextAccess::getDefaultSettings() const
{
    std::lock_guard lock{mutex};
    return enabled_settings ? enabled_settings->getSettings() : nullptr;
}


std::shared_ptr<const SettingsConstraints> ContextAccess::getSettingsConstraints() const
{
    std::lock_guard lock{mutex};
    return enabled_settings ? enabled_settings->getConstraints() : nullptr;
}


std::shared_ptr<const AccessRights> ContextAccess::getAccess() const
{
    std::lock_guard lock{mutex};
    return access;
}


template <bool grant_option, typename... Args>
bool ContextAccess::isGrantedImpl2(const AccessFlags & flags, const Args &... args) const
{
    bool access_granted;
    if constexpr (grant_option)
        access_granted = getAccess()->hasGrantOption(flags, args...);
    else
        access_granted = getAccess()->isGranted(flags, args...);

    if (trace_log)
        LOG_TRACE(trace_log, "Access {}: {}{}", (access_granted ? "granted" : "denied"), (AccessRightsElement{flags, args...}.toString()),
                  (grant_option ? " WITH GRANT OPTION" : ""));

    return access_granted;
}

template <bool grant_option>
bool ContextAccess::isGrantedImpl(const AccessFlags & flags) const
{
    return isGrantedImpl2<grant_option>(flags);
}

template <bool grant_option, typename... Args>
bool ContextAccess::isGrantedImpl(const AccessFlags & flags, const std::string_view & database, const Args &... args) const
{
    return isGrantedImpl2<grant_option>(flags, database.empty() ? params.current_database : database, args...);
}

template <bool grant_option>
bool ContextAccess::isGrantedImpl(const AccessRightsElement & element) const
{
    if (element.any_database)
        return isGrantedImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        return isGrantedImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
bool ContextAccess::isGrantedImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl<grant_option>(element))
            return false;
    return true;
}

bool ContextAccess::isGranted(const AccessFlags & flags) const { return isGrantedImpl<false>(flags); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<false>(flags, database); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<false>(flags, database, table); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<false>(flags, database, table, column); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessRightsElement & element) const { return isGrantedImpl<false>(element); }
bool ContextAccess::isGranted(const AccessRightsElements & elements) const { return isGrantedImpl<false>(elements); }

bool ContextAccess::hasGrantOption(const AccessFlags & flags) const { return isGrantedImpl<true>(flags); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<true>(flags, database); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<true>(flags, database, table); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<true>(flags, database, table, column); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const AccessRightsElement & element) const { return isGrantedImpl<true>(element); }
bool ContextAccess::hasGrantOption(const AccessRightsElements & elements) const { return isGrantedImpl<true>(elements); }


template <bool grant_option, typename... Args>
void ContextAccess::checkAccessImpl2(const AccessFlags & flags, const Args &... args) const
{
    if constexpr (grant_option)
    {
        if (hasGrantOption(flags, args...))
            return;
    }
    else
    {
        if (isGranted(flags, args...))
            return;
    }

    auto show_error = [&](const String & msg, int error_code)
    {
        throw Exception(user_name + ": " + msg, error_code);
    };

    std::lock_guard lock{mutex};

    if (!user)
        show_error("User has been dropped", ErrorCodes::UNKNOWN_USER);

    if (grant_option && access->isGranted(flags, args...))
    {
        show_error(
            "Not enough privileges. "
            "The required privileges have been granted, but without grant option. "
            "To execute this query it's necessary to have the grant "
                + AccessRightsElement{flags, args...}.toString() + " WITH GRANT OPTION",
            ErrorCodes::ACCESS_DENIED);
    }

    if (params.readonly)
    {
        if (!access_without_readonly)
        {
            Params changed_params = params;
            changed_params.readonly = 0;
            access_without_readonly = std::make_shared<AccessRights>(calculateFinalAccessRights(*access_from_user_and_roles, changed_params));
        }

        if (access_without_readonly->isGranted(flags, args...))
        {
            if (params.interface == ClientInfo::Interface::HTTP && params.http_method == ClientInfo::HTTPMethod::GET)
                show_error(
                    "Cannot execute query in readonly mode. "
                    "For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
                    ErrorCodes::READONLY);
            else
                show_error("Cannot execute query in readonly mode", ErrorCodes::READONLY);
        }
    }

    if (!params.allow_ddl)
    {
        if (!access_with_allow_ddl)
        {
            Params changed_params = params;
            changed_params.allow_ddl = true;
            access_with_allow_ddl = std::make_shared<AccessRights>(calculateFinalAccessRights(*access_from_user_and_roles, changed_params));
        }

        if (access_with_allow_ddl->isGranted(flags, args...))
        {
            show_error("Cannot execute query. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
        }
    }

    if (!params.allow_introspection)
    {
        if (!access_with_allow_introspection)
        {
            Params changed_params = params;
            changed_params.allow_introspection = true;
            access_with_allow_introspection = std::make_shared<AccessRights>(calculateFinalAccessRights(*access_from_user_and_roles, changed_params));
        }

        if (access_with_allow_introspection->isGranted(flags, args...))
        {
            show_error("Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);
        }
    }

    show_error(
        "Not enough privileges. To execute this query it's necessary to have the grant "
            + AccessRightsElement{flags, args...}.toString() + (grant_option ? " WITH GRANT OPTION" : ""),
        ErrorCodes::ACCESS_DENIED);
}

template <bool grant_option>
void ContextAccess::checkAccessImpl(const AccessFlags & flags) const
{
    checkAccessImpl2<grant_option>(flags);
}

template <bool grant_option, typename... Args>
void ContextAccess::checkAccessImpl(const AccessFlags & flags, const std::string_view & database, const Args &... args) const
{
    checkAccessImpl2<grant_option>(flags, database.empty() ? params.current_database : database, args...);
}

template <bool grant_option>
void ContextAccess::checkAccessImpl(const AccessRightsElement & element) const
{
    if (element.any_database)
        checkAccessImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        checkAccessImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        checkAccessImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        checkAccessImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
void ContextAccess::checkAccessImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        checkAccessImpl<grant_option>(element);
}

void ContextAccess::checkAccess(const AccessFlags & flags) const { checkAccessImpl<false>(flags); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<false>(flags, database); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<false>(flags, database, table); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<false>(flags, database, table, column); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<false>(flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<false>(flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessRightsElement & element) const { checkAccessImpl<false>(element); }
void ContextAccess::checkAccess(const AccessRightsElements & elements) const { checkAccessImpl<false>(elements); }

void ContextAccess::checkGrantOption(const AccessFlags & flags) const { checkAccessImpl<true>(flags); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<true>(flags, database); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<true>(flags, database, table); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<true>(flags, database, table, column); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<true>(flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<true>(flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessRightsElement & element) const { checkAccessImpl<true>(element); }
void ContextAccess::checkGrantOption(const AccessRightsElements & elements) const { checkAccessImpl<true>(elements); }


template <typename Container, typename GetNameFunction>
bool ContextAccess::checkAdminOptionImpl(bool throw_on_error, const Container & role_ids, const GetNameFunction & get_name_function) const
{
    if (isGranted(AccessType::ROLE_ADMIN))
        return true;

    auto info = getRolesInfo();
    if (!info)
    {
        if (!user)
        {
            if (throw_on_error)
                throw Exception(user_name + ": User has been dropped", ErrorCodes::UNKNOWN_USER);
            else
                return false;
        }
        return true;
    }

    size_t i = 0;
    for (auto it = std::begin(role_ids); it != std::end(role_ids); ++it, ++i)
    {
        const UUID & role_id = *it;
        if (info->enabled_roles_with_admin_option.count(role_id))
            continue;

        auto role_name = get_name_function(role_id, i);
        if (!role_name)
            role_name = "ID {" + toString(role_id) + "}";
        String msg = "To execute this query it's necessary to have the role " + backQuoteIfNeed(*role_name) + " granted with ADMIN option";
        if (info->enabled_roles.count(role_id))
            msg = "Role " + backQuote(*role_name) + " is granted, but without ADMIN option. " + msg;
        if (throw_on_error)
            throw Exception(getUserName() + ": Not enough privileges. " + msg, ErrorCodes::ACCESS_DENIED);
        else
            return false;
    }

    return true;
}

bool ContextAccess::hasAdminOption(const UUID & role_id) const
{
    return checkAdminOptionImpl(false, to_array(role_id), [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

bool ContextAccess::hasAdminOption(const UUID & role_id, const String & role_name) const
{
    return checkAdminOptionImpl(false, to_array(role_id), [&role_name](const UUID &, size_t) { return std::optional<String>{role_name}; });
}

bool ContextAccess::hasAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImpl(false, to_array(role_id), [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids) const
{
    return checkAdminOptionImpl(false, role_ids, [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const
{
    return checkAdminOptionImpl(false, role_ids, [&names_of_roles](const UUID &, size_t i) { return std::optional<String>{names_of_roles[i]}; });
}

bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImpl(false, role_ids, [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

void ContextAccess::checkAdminOption(const UUID & role_id) const
{
    checkAdminOptionImpl(true, to_array(role_id), [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

void ContextAccess::checkAdminOption(const UUID & role_id, const String & role_name) const
{
    checkAdminOptionImpl(true, to_array(role_id), [&role_name](const UUID &, size_t) { return std::optional<String>{role_name}; });
}

void ContextAccess::checkAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const
{
    checkAdminOptionImpl(true, to_array(role_id), [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids) const
{
    checkAdminOptionImpl(true, role_ids, [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const
{
    checkAdminOptionImpl(true, role_ids, [&names_of_roles](const UUID &, size_t i) { return std::optional<String>{names_of_roles[i]}; });
}

void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const
{
    checkAdminOptionImpl(true, role_ids, [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

}
