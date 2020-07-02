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
    setFinalAccess();
}


void ContextAccess::setFinalAccess() const
{
    auto final_access = std::make_shared<AccessRights>();
    *final_access = user->access;
    if (roles_info)
        final_access->merge(roles_info->access);

    static const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
        | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
        | AccessType::TRUNCATE;

    static const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY;
    static const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
    static const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;
    static const AccessFlags write_dcl_access = AccessType::ACCESS_MANAGEMENT - AccessType::SHOW_ACCESS;

    if (params.readonly)
        final_access->revoke(write_table_access | table_and_dictionary_ddl | write_dcl_access | AccessType::SYSTEM | AccessType::KILL_QUERY);

    if (params.readonly == 1)
    {
        /// Table functions are forbidden in readonly mode.
        /// For example, for readonly = 2 - allowed.
        final_access->revoke(AccessType::CREATE_TEMPORARY_TABLE);
    }

    if (!params.allow_ddl)
        final_access->revoke(table_and_dictionary_ddl);

    if (!params.allow_introspection)
        final_access->revoke(AccessType::INTROSPECTION);

    /// Anyone has access to the "system" database.
    final_access->grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE);

    if (params.readonly != 1)
    {
        /// User has access to temporary or external table if such table was resolved in session or query context
        final_access->grant(AccessFlags::allTableFlags() | AccessFlags::allColumnFlags(), DatabaseCatalog::TEMPORARY_DATABASE);
    }

    if (params.readonly)
    {
        /// No grant option in readonly mode.
        final_access->revokeGrantOption(AccessType::ALL);
    }

    if (trace_log)
    {
        if (roles_info && !roles_info->getCurrentRolesNames().empty())
        {
            LOG_TRACE(trace_log, "Current_roles: {}, enabled_roles: {}",
                boost::algorithm::join(roles_info->getCurrentRolesNames(), ", "),
                boost::algorithm::join(roles_info->getEnabledRolesNames(), ", "));
        }
        LOG_TRACE(trace_log, "Settings: readonly={}, allow_ddl={}, allow_introspection_functions={}", params.readonly, params.allow_ddl, params.allow_introspection);
        LOG_TRACE(trace_log, "List of all grants: {}", final_access->toString());
    }

    access = final_access;
}


bool ContextAccess::isCorrectPassword(const String & password) const
{
    std::lock_guard lock{mutex};
    if (!user)
        return false;
    return user->authentication.isCorrectPassword(password);
}

bool ContextAccess::isClientHostAllowed() const
{
    std::lock_guard lock{mutex};
    if (!user)
        return false;
    return user->allowed_client_hosts.contains(params.address);
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
    {
        show_error("User has been dropped", ErrorCodes::UNKNOWN_USER);
    }

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
            access_without_readonly = manager->getContextAccess(changed_params);
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
            access_with_allow_ddl = manager->getContextAccess(changed_params);
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
            access_with_allow_introspection = manager->getContextAccess(changed_params);
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


void ContextAccess::checkAdminOption(const UUID & role_id) const
{
    if (isGranted(AccessType::ROLE_ADMIN))
        return;

    auto info = getRolesInfo();
    if (info && info->enabled_roles_with_admin_option.count(role_id))
        return;

    if (!user)
        throw Exception(user_name + ": User has been dropped", ErrorCodes::UNKNOWN_USER);

    std::optional<String> role_name = manager->readName(role_id);
    if (!role_name)
        role_name = "ID {" + toString(role_id) + "}";
    throw Exception(
        getUserName() + ": Not enough privileges. To execute this query it's necessary to have the grant " + backQuoteIfNeed(*role_name)
            + " WITH ADMIN OPTION ",
        ErrorCodes::ACCESS_DENIED);
}

}
