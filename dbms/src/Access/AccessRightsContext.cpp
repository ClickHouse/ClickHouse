#include <Access/AccessRightsContext.h>
#include <Access/AccessControlManager.h>
#include <Access/RowPolicyContext.h>
#include <Access/QuotaContext.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/range/algorithm/fill.hpp>
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


AccessRightsContext::AccessRightsContext()
{
    auto everything_granted = boost::make_shared<AccessRights>();
    everything_granted->grant(AccessType::ALL);
    boost::range::fill(result_access_cache, everything_granted);
    row_policy_context = std::make_shared<RowPolicyContext>();
    quota_context = std::make_shared<QuotaContext>();
}


AccessRightsContext::AccessRightsContext(const AccessControlManager & manager_, const Params & params_)
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


void AccessRightsContext::setUser(const UserPtr & user_) const
{
    user = user_;
    if (!user)
    {
        /// User has been dropped.
        auto nothing_granted = boost::make_shared<AccessRights>();
        boost::range::fill(result_access_cache, nothing_granted);
        subscription_for_user_change = {};
        row_policy_context = std::make_shared<RowPolicyContext>();
        quota_context = std::make_shared<QuotaContext>();
        return;
    }

    user_name = user->getName();
    trace_log = &Poco::Logger::get("AccessRightsContext (" + user_name + ")");
    boost::range::fill(result_access_cache, nullptr /* need recalculate */);
    row_policy_context = manager->getRowPolicyContext(*params.user_id);
    quota_context = manager->getQuotaContext(*params.user_id, user_name, params.address, params.quota_key);
}


void AccessRightsContext::checkPassword(const String & password) const
{
    std::lock_guard lock{mutex};
    if (!user)
        throw Exception(user_name + ": User has been dropped", ErrorCodes::UNKNOWN_USER);
    user->authentication.checkPassword(password, user_name);
}

void AccessRightsContext::checkHostIsAllowed() const
{
    std::lock_guard lock{mutex};
    if (!user)
        throw Exception(user_name + ": User has been dropped", ErrorCodes::UNKNOWN_USER);
    user->allowed_client_hosts.checkContains(params.address, user_name);
}


template <int mode, bool grant_option, typename... Args>
bool AccessRightsContext::checkAccessImpl(Poco::Logger * log_, const AccessFlags & access, const Args &... args) const
{
    auto result_access = calculateResultAccess(grant_option);
    bool is_granted = result_access->isGranted(access, args...);

    if (trace_log)
        LOG_TRACE(trace_log, "Access " << (is_granted ? "granted" : "denied") << ": " << (AccessRightsElement{access, args...}.toString()));

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
    else if (grant_option && calculateResultAccess(false, params.readonly, params.allow_ddl, params.allow_introspection)->isGranted(access, args...))
    {
        show_error(
            "Not enough privileges. "
            "The required privileges have been granted, but without grant option. "
            "To execute this query it's necessary to have the grant "
                + AccessRightsElement{access, args...}.toString() + " WITH GRANT OPTION",
            ErrorCodes::ACCESS_DENIED);
    }
    else if (params.readonly && calculateResultAccess(false, false, params.allow_ddl, params.allow_introspection)->isGranted(access, args...))
    {
        if (params.interface == ClientInfo::Interface::HTTP && params.http_method == ClientInfo::HTTPMethod::GET)
            show_error(
                "Cannot execute query in readonly mode. "
                "For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
                ErrorCodes::READONLY);
        else
            show_error("Cannot execute query in readonly mode", ErrorCodes::READONLY);
    }
    else if (!params.allow_ddl && calculateResultAccess(false, params.readonly, true, params.allow_introspection)->isGranted(access, args...))
    {
        show_error("Cannot execute query. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
    }
    else if (!params.allow_introspection && calculateResultAccess(false, params.readonly, params.allow_ddl, true)->isGranted(access, args...))
    {
        show_error("Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);
    }
    else
    {
        show_error(
            "Not enough privileges. To execute this query it's necessary to have the grant "
                + AccessRightsElement{access, args...}.toString() + (grant_option ? " WITH GRANT OPTION" : ""),
            ErrorCodes::ACCESS_DENIED);
    }

    return false;
}


template <int mode, bool grant_option>
bool AccessRightsContext::checkAccessImpl(Poco::Logger * log_, const AccessRightsElement & element) const
{
    if (element.any_database)
    {
        return checkAccessImpl<mode, grant_option>(log_, element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, params.current_database);
        else
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, params.current_database, element.table);
        else
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, params.current_database, element.table, element.columns);
        else
            return checkAccessImpl<mode, grant_option>(log_, element.access_flags, element.database, element.table, element.columns);
    }
}


template <int mode, bool grant_option>
bool AccessRightsContext::checkAccessImpl(Poco::Logger * log_, const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!checkAccessImpl<mode, grant_option>(log_, element))
            return false;
    return true;
}


void AccessRightsContext::checkAccess(const AccessFlags & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access); }
void AccessRightsContext::checkAccess(const AccessFlags & access, const std::string_view & database) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access, database); }
void AccessRightsContext::checkAccess(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access, database, table); }
void AccessRightsContext::checkAccess(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access, database, table, column); }
void AccessRightsContext::checkAccess(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access, database, table, columns); }
void AccessRightsContext::checkAccess(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access, database, table, columns); }
void AccessRightsContext::checkAccess(const AccessRightsElement & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access); }
void AccessRightsContext::checkAccess(const AccessRightsElements & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, false>(nullptr, access); }

bool AccessRightsContext::isGranted(const AccessFlags & access) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access, database); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access, database, table); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access, database, table, column); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access, database, table, columns); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access, database, table, columns); }
bool AccessRightsContext::isGranted(const AccessRightsElement & access) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access); }
bool AccessRightsContext::isGranted(const AccessRightsElements & access) const { return checkAccessImpl<RETURN_FALSE_IF_ACCESS_DENIED, false>(nullptr, access); }

bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access, database); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access, database, table); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access, database, table, column); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access, database, table, columns); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access, database, table, columns); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessRightsElement & access) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessRightsElements & access) const { return checkAccessImpl<LOG_WARNING_IF_ACCESS_DENIED, false>(log_, access); }

void AccessRightsContext::checkGrantOption(const AccessFlags & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access); }
void AccessRightsContext::checkGrantOption(const AccessFlags & access, const std::string_view & database) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access, database); }
void AccessRightsContext::checkGrantOption(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access, database, table); }
void AccessRightsContext::checkGrantOption(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access, database, table, column); }
void AccessRightsContext::checkGrantOption(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access, database, table, columns); }
void AccessRightsContext::checkGrantOption(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access, database, table, columns); }
void AccessRightsContext::checkGrantOption(const AccessRightsElement & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access); }
void AccessRightsContext::checkGrantOption(const AccessRightsElements & access) const { checkAccessImpl<THROW_IF_ACCESS_DENIED, true>(nullptr, access); }


boost::shared_ptr<const AccessRights> AccessRightsContext::calculateResultAccess(bool grant_option) const
{
    return calculateResultAccess(grant_option, params.readonly, params.allow_ddl, params.allow_introspection);
}


boost::shared_ptr<const AccessRights> AccessRightsContext::calculateResultAccess(bool grant_option, UInt64 readonly_, bool allow_ddl_, bool allow_introspection_) const
{
    size_t cache_index = static_cast<size_t>(readonly_ != params.readonly)
                       + static_cast<size_t>(allow_ddl_ != params.allow_ddl) * 2 +
                       + static_cast<size_t>(allow_introspection_ != params.allow_introspection) * 3
                       + static_cast<size_t>(grant_option) * 4;
    assert(cache_index < std::size(result_access_cache));
    auto cached = result_access_cache[cache_index].load();
    if (cached)
        return cached;

    std::lock_guard lock{mutex};
    cached = result_access_cache[cache_index].load();
    if (cached)
        return cached;

    auto result_ptr = boost::make_shared<AccessRights>();
    auto & result = *result_ptr;

    result = grant_option ? user->access_with_grant_option : user->access;

    static const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
        | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
        | AccessType::DETACH_DATABASE | AccessType::DETACH_TABLE | AccessType::DETACH_VIEW | AccessType::TRUNCATE;
    static const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY | AccessType::DETACH_DICTIONARY;
    static const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
    static const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;

    /// Anyone has access to the "system" database.
    result.grant(AccessType::SELECT, "system");

    if (readonly_)
        result.fullRevoke(write_table_access | AccessType::SYSTEM);

    if (readonly_ || !allow_ddl_)
        result.fullRevoke(table_and_dictionary_ddl);

    if (readonly_ && grant_option)
        result.fullRevoke(AccessType::ALL);

    if (readonly_ == 1)
    {
        /// Table functions are forbidden in readonly mode.
        /// For example, for readonly = 2 - allowed.
        result.fullRevoke(AccessType::CREATE_TEMPORARY_TABLE | AccessType::TABLE_FUNCTIONS);
    }

    if (!allow_introspection_)
        result.fullRevoke(AccessType::INTROSPECTION);

    result_access_cache[cache_index].store(result_ptr);

    if (trace_log && (params.readonly == readonly_) && (params.allow_ddl == allow_ddl_) && (params.allow_introspection == allow_introspection_))
        LOG_TRACE(trace_log, "List of all grants: " << result_ptr->toString() << (grant_option ? " WITH GRANT OPTION" : ""));

    return result_ptr;
}


UserPtr AccessRightsContext::getUser() const
{
    std::lock_guard lock{mutex};
    return user;
}

String AccessRightsContext::getUserName() const
{
    std::lock_guard lock{mutex};
    return user_name;
}

RowPolicyContextPtr AccessRightsContext::getRowPolicy() const
{
    std::lock_guard lock{mutex};
    return row_policy_context;
}

QuotaContextPtr AccessRightsContext::getQuota() const
{
    std::lock_guard lock{mutex};
    return quota_context;
}


bool operator <(const AccessRightsContext::Params & lhs, const AccessRightsContext::Params & rhs)
{
#define ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(field) \
    if (lhs.field < rhs.field) \
        return true; \
    if (lhs.field > rhs.field) \
        return false
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(user_id);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(address);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(quota_key);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(current_database);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(readonly);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(allow_ddl);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(allow_introspection);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(interface);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(http_method);
    return false;
#undef ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER
}


bool operator ==(const AccessRightsContext::Params & lhs, const AccessRightsContext::Params & rhs)
{
#define ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(field) \
    if (lhs.field != rhs.field) \
        return false
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(user_id);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(address);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(quota_key);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(current_database);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(readonly);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(allow_ddl);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(allow_introspection);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(interface);
    ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER(http_method);
    return true;
#undef ACCESS_RIGHTS_CONTEXT_PARAMS_COMPARE_HELPER
}

}
