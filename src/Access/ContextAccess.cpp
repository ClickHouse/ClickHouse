#include <Access/ContextAccess.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledRoles.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/EnabledQuota.h>
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
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/range/algorithm/fill.hpp>
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
    enum CheckAccessRightsMode
    {
        RETURN_FALSE_IF_ACCESS_DENIED,
        LOG_WARNING_IF_ACCESS_DENIED,
        THROW_IF_ACCESS_DENIED,
    };


    String formatSkippedMessage()
    {
        return "";
    }

    String formatSkippedMessage(const std::string_view & database)
    {
        return ". Skipped database " + backQuoteIfNeed(database);
    }

    String formatSkippedMessage(const std::string_view & database, const std::string_view & table)
    {
        String str = ". Skipped table ";
        if (!database.empty())
            str += backQuoteIfNeed(database) + ".";
        str += backQuoteIfNeed(table);
        return str;
    }

    String formatSkippedMessage(const std::string_view & database, const std::string_view & table, const std::string_view & column)
    {
        String str = ". Skipped column " + backQuoteIfNeed(column) + " ON ";
        if (!database.empty())
            str += backQuoteIfNeed(database) + ".";
        str += backQuoteIfNeed(table);
        return str;
    }

    template <typename StringT>
    String formatSkippedMessage(const std::string_view & database, const std::string_view & table, const std::vector<StringT> & columns)
    {
        if (columns.size() == 1)
            return formatSkippedMessage(database, table, columns[0]);

        String str = ". Skipped columns ";
        bool need_comma = false;
        for (const auto & column : columns)
        {
            if (std::exchange(need_comma, true))
                str += ", ";
            str += backQuoteIfNeed(column);
        }
        str += " ON ";
        if (!database.empty())
            str += backQuoteIfNeed(database) + ".";
        str += backQuoteIfNeed(table);
        return str;
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
        auto nothing_granted = boost::make_shared<AccessRights>();
        boost::range::fill(result_access, nothing_granted);
        subscription_for_user_change = {};
        subscription_for_roles_changes = {};
        enabled_roles = nullptr;
        roles_info = nullptr;
        roles_with_admin_option = nullptr;
        enabled_row_policies = nullptr;
        enabled_quota = nullptr;
        enabled_settings = nullptr;
        return;
    }

    user_name = user->getName();
    trace_log = &Poco::Logger::get("ContextAccess (" + user_name + ")");

    std::vector<UUID> current_roles, current_roles_with_admin_option;
    if (params.use_default_roles)
    {
        for (const UUID & id : user->granted_roles)
        {
            if (user->default_roles.match(id))
                current_roles.push_back(id);
        }
        boost::range::set_intersection(current_roles, user->granted_roles_with_admin_option,
                                       std::back_inserter(current_roles_with_admin_option));
    }
    else
    {
        current_roles.reserve(params.current_roles.size());
        for (const auto & id : params.current_roles)
        {
            if (user->granted_roles.count(id))
                current_roles.push_back(id);
            if (user->granted_roles_with_admin_option.count(id))
                current_roles_with_admin_option.push_back(id);
        }
    }

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
    roles_with_admin_option.store(boost::make_shared<boost::container::flat_set<UUID>>(roles_info->enabled_roles_with_admin_option.begin(), roles_info->enabled_roles_with_admin_option.end()));
    boost::range::fill(result_access, nullptr /* need recalculate */);
    enabled_row_policies = manager->getEnabledRowPolicies(*params.user_id, roles_info->enabled_roles);
    enabled_quota = manager->getEnabledQuota(*params.user_id, user_name, roles_info->enabled_roles, params.address, params.quota_key);
    enabled_settings = manager->getEnabledSettings(*params.user_id, user->settings, roles_info->enabled_roles, roles_info->settings_from_enabled_roles);
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


template <int mode, bool grant_option, typename... Args>
bool ContextAccess::calculateResultAccessAndCheck(Poco::Logger * log_, const AccessFlags & flags, const Args &... args) const
{
    auto access = calculateResultAccess(grant_option);
    bool is_granted = access->isGranted(flags, args...);

    if (trace_log)
        LOG_TRACE(trace_log, "Access " << (is_granted ? "granted" : "denied") << ": " << (AccessRightsElement{flags, args...}.toString()));

    if (is_granted)
        return true;

    if constexpr (mode == RETURN_FALSE_IF_ACCESS_DENIED)
        return false;

    if constexpr (mode == LOG_WARNING_IF_ACCESS_DENIED)
    {
        if (!log_)
            return false;
    }

    auto show_error = [&](const String & msg, [[maybe_unused]] int error_code)
    {
        if constexpr (mode == THROW_IF_ACCESS_DENIED)
            throw Exception(user_name + ": " + msg, error_code);
        else if constexpr (mode == LOG_WARNING_IF_ACCESS_DENIED)
            LOG_WARNING(log_, user_name + ": " + msg + formatSkippedMessage(args...));
    };

    if (!user)
    {
        show_error("User has been dropped", ErrorCodes::UNKNOWN_USER);
    }
    else if (grant_option && calculateResultAccess(false, params.readonly, params.allow_ddl, params.allow_introspection)->isGranted(flags, args...))
    {
        show_error(
            "Not enough privileges. "
            "The required privileges have been granted, but without grant option. "
            "To execute this query it's necessary to have the grant "
                + AccessRightsElement{flags, args...}.toString() + " WITH GRANT OPTION",
            ErrorCodes::ACCESS_DENIED);
    }
    else if (params.readonly && calculateResultAccess(false, false, params.allow_ddl, params.allow_introspection)->isGranted(flags, args...))
    {
        if (params.interface == ClientInfo::Interface::HTTP && params.http_method == ClientInfo::HTTPMethod::GET)
            show_error(
                "Cannot execute query in readonly mode. "
                "For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
                ErrorCodes::READONLY);
        else
            show_error("Cannot execute query in readonly mode", ErrorCodes::READONLY);
    }
    else if (!params.allow_ddl && calculateResultAccess(false, params.readonly, true, params.allow_introspection)->isGranted(flags, args...))
    {
        show_error("Cannot execute query. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
    }
    else if (!params.allow_introspection && calculateResultAccess(false, params.readonly, params.allow_ddl, true)->isGranted(flags, args...))
    {
        show_error("Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);
    }
    else
    {
        show_error(
            "Not enough privileges. To execute this query it's necessary to have the grant "
                + AccessRightsElement{flags, args...}.toString() + (grant_option ? " WITH GRANT OPTION" : ""),
            ErrorCodes::ACCESS_DENIED);
    }

    return false;
}


template <int mode, bool grant_option>
bool ContextAccess::checkAccessImpl(Poco::Logger * log_, const AccessFlags & flags) const
{
    return calculateResultAccessAndCheck<mode, grant_option>(log_, flags);
}

template <int mode, bool grant_option, typename... Args>
bool ContextAccess::checkAccessImpl(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const Args &... args) const
{
    if (database.empty())
        return calculateResultAccessAndCheck<mode, grant_option>(log_, flags, params.current_database, args...);
    else
        return calculateResultAccessAndCheck<mode, grant_option>(log_, flags, database, args...);
}


template <int mode, bool grant_option>
bool ContextAccess::checkAccessImpl(Poco::Logger * log_, const AccessRightsElement & element) const
{
    if (element.any_database)
    {
        return checkAccessImpl<mode, grant_option>(log_, element.access_flags);
    }
    else if (element.any_table)
    {
        return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database, element.table);
    }
    else
    {
        return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database, element.table, element.columns);
    }
}


template <int mode, bool grant_option>
bool ContextAccess::checkAccessImpl(Poco::Logger * log_, const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!checkAccessImpl<mode, grant_option>(log_, element))
            return false;
    return true;
}


void ContextAccess::checkAccess(const AccessFlags & flags) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags, database); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags, database, table); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, column); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessRightsElement & element) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, element); }
void ContextAccess::checkAccess(const AccessRightsElements & elements) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, elements); }

bool ContextAccess::isGranted(const AccessFlags & flags) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags, database); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags, database, table); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, column); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessRightsElement & element) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, element); }
bool ContextAccess::isGranted(const AccessRightsElements & elements) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, elements); }

bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags, database); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags, database, table); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags, database, table, column); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags, database, table, columns); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, flags, database, table, columns); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessRightsElement & element) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, element); }
bool ContextAccess::isGranted(Poco::Logger * log_, const AccessRightsElements & elements) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, elements); }

void ContextAccess::checkGrantOption(const AccessFlags & flags) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags, database); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags, database, table); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags, database, table, column); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessRightsElement & element) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, element); }
void ContextAccess::checkGrantOption(const AccessRightsElements & elements) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, elements); }


void ContextAccess::checkAdminOption(const UUID & role_id) const
{
    if (isGranted(AccessType::ROLE_ADMIN))
        return;

    auto roles_with_admin_option_loaded = roles_with_admin_option.load();
    if (roles_with_admin_option_loaded && roles_with_admin_option_loaded->count(role_id))
        return;

    std::optional<String> role_name = manager->readName(role_id);
    if (!role_name)
        role_name = "ID {" + toString(role_id) + "}";
    throw Exception(
        getUserName() + ": Not enough privileges. To execute this query it's necessary to have the grant " + backQuoteIfNeed(*role_name)
            + " WITH ADMIN OPTION ",
        ErrorCodes::ACCESS_DENIED);
}


boost::shared_ptr<const AccessRights> ContextAccess::calculateResultAccess(bool grant_option) const
{
    return calculateResultAccess(grant_option, params.readonly, params.allow_ddl, params.allow_introspection);
}


boost::shared_ptr<const AccessRights> ContextAccess::calculateResultAccess(bool grant_option, UInt64 readonly_, bool allow_ddl_, bool allow_introspection_) const
{
    size_t index = static_cast<size_t>(readonly_ != params.readonly)
                       + static_cast<size_t>(allow_ddl_ != params.allow_ddl) * 2 +
                       + static_cast<size_t>(allow_introspection_ != params.allow_introspection) * 3
                       + static_cast<size_t>(grant_option) * 4;
    assert(index < std::size(result_access));
    auto res = result_access[index].load();
    if (res)
        return res;

    std::lock_guard lock{mutex};
    res = result_access[index].load();
    if (res)
        return res;

    auto merged_access = boost::make_shared<AccessRights>();

    if (grant_option)
    {
        *merged_access = user->access_with_grant_option;
        if (roles_info)
            merged_access->merge(roles_info->access_with_grant_option);
    }
    else
    {
        *merged_access = user->access;
        if (roles_info)
            merged_access->merge(roles_info->access);
    }

    static const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
        | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
        | AccessType::TRUNCATE;

    static const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY;
    static const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
    static const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;
    static const AccessFlags write_dcl_access = AccessType::ACCESS_MANAGEMENT - AccessType::SHOW_ACCESS;

    if (readonly_)
        merged_access->revoke(write_table_access | table_and_dictionary_ddl | write_dcl_access | AccessType::SYSTEM | AccessType::KILL_QUERY);

    if (readonly_ == 1)
    {
        /// Table functions are forbidden in readonly mode.
        /// For example, for readonly = 2 - allowed.
        merged_access->revoke(AccessType::CREATE_TEMPORARY_TABLE);
    }

    if (!allow_ddl_ && !grant_option)
        merged_access->revoke(table_and_dictionary_ddl);

    if (!allow_introspection_ && !grant_option)
        merged_access->revoke(AccessType::INTROSPECTION);

    /// Anyone has access to the "system" database.
    merged_access->grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE);

    if (readonly_ != 1)
    {
        /// User has access to temporary or external table if such table was resolved in session or query context
        merged_access->grant(AccessFlags::allTableFlags() | AccessFlags::allColumnFlags(), DatabaseCatalog::TEMPORARY_DATABASE);
    }

    if (readonly_ && grant_option)
    {
        /// No grant option in readonly mode.
        merged_access->revoke(AccessType::ALL);
    }

    if (trace_log && (params.readonly == readonly_) && (params.allow_ddl == allow_ddl_) && (params.allow_introspection == allow_introspection_))
    {
        LOG_TRACE(trace_log, "List of all grants: " << merged_access->toString() << (grant_option ? " WITH GRANT OPTION" : ""));
        if (roles_info && !roles_info->getCurrentRolesNames().empty())
        {
            LOG_TRACE(
                trace_log,
                "Current_roles: " << boost::algorithm::join(roles_info->getCurrentRolesNames(), ", ")
                                  << ", enabled_roles: " << boost::algorithm::join(roles_info->getEnabledRolesNames(), ", "));
        }
        LOG_TRACE(trace_log, "Settings: readonly=" << readonly_ << ", allow_ddl=" << allow_ddl_ << ", allow_introspection_functions=" << allow_introspection_);
    }

    res = std::move(merged_access);
    result_access[index].store(res);
    return res;
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

std::vector<UUID> ContextAccess::getCurrentRoles() const
{
    std::lock_guard lock{mutex};
    return roles_info ? roles_info->current_roles : std::vector<UUID>{};
}

Strings ContextAccess::getCurrentRolesNames() const
{
    std::lock_guard lock{mutex};
    return roles_info ? roles_info->getCurrentRolesNames() : Strings{};
}

std::vector<UUID> ContextAccess::getEnabledRoles() const
{
    std::lock_guard lock{mutex};
    return roles_info ? roles_info->enabled_roles : std::vector<UUID>{};
}

Strings ContextAccess::getEnabledRolesNames() const
{
    std::lock_guard lock{mutex};
    return roles_info ? roles_info->getEnabledRolesNames() : Strings{};
}

std::shared_ptr<const EnabledRowPolicies> ContextAccess::getRowPolicies() const
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


std::shared_ptr<const ContextAccess> ContextAccess::getFullAccess()
{
    static const std::shared_ptr<const ContextAccess> res = []
    {
        auto full_access = std::shared_ptr<ContextAccess>(new ContextAccess);
        auto everything_granted = boost::make_shared<AccessRights>();
        everything_granted->grant(AccessType::ALL);
        boost::range::fill(full_access->result_access, everything_granted);
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

}
