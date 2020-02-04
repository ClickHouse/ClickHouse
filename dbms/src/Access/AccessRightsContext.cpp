#include <Access/AccessRightsContext.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <assert.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int FUNCTION_NOT_ALLOWED;
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
    result_access_cache[0] = std::move(everything_granted);
}


AccessRightsContext::AccessRightsContext(const ClientInfo & client_info_, const AccessRights & granted_to_user_, const Settings & settings, const String & current_database_)
    : user_name(client_info_.current_user)
    , granted_to_user(granted_to_user_)
    , readonly(settings.readonly)
    , allow_ddl(settings.allow_ddl)
    , allow_introspection(settings.allow_introspection_functions)
    , current_database(current_database_)
    , interface(client_info_.interface)
    , http_method(client_info_.http_method)
    , trace_log(&Poco::Logger::get("AccessRightsContext (" + user_name + ")"))
{
}


template <int mode, typename... Args>
bool AccessRightsContext::checkImpl(Poco::Logger * log_, const AccessFlags & access, const Args &... args) const
{
    auto result_access = calculateResultAccess();
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
            throw Exception(msg, error_code);
        else if constexpr (mode == LOG_WARNING_IF_ACCESS_DENIED)
            LOG_WARNING(log_, msg + formatSkippedMessage(args...));
    };

    if (readonly && calculateResultAccess(false, allow_ddl, allow_introspection)->isGranted(access, args...))
    {
        if (interface == ClientInfo::Interface::HTTP && http_method == ClientInfo::HTTPMethod::GET)
            show_error(
                "Cannot execute query in readonly mode. "
                "For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
                ErrorCodes::READONLY);
        else
            show_error("Cannot execute query in readonly mode", ErrorCodes::READONLY);
    }
    else if (!allow_ddl && calculateResultAccess(readonly, true, allow_introspection)->isGranted(access, args...))
    {
        show_error("Cannot execute query. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
    }
    else if (!allow_introspection && calculateResultAccess(readonly, allow_ddl, true)->isGranted(access, args...))
    {
        show_error("Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);
    }
    else
    {
        show_error(
            user_name + ": Not enough privileges. To perform this operation you should have grant "
                + AccessRightsElement{access, args...}.toString(),
            ErrorCodes::ACCESS_DENIED);
    }

    return false;
}

template <int mode>
bool AccessRightsContext::checkImpl(Poco::Logger * log_, const AccessRightsElement & element) const
{
    if (element.any_database)
    {
        return checkImpl<mode>(log_, element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            return checkImpl<mode>(log_, element.access_flags, current_database);
        else
            return checkImpl<mode>(log_, element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            return checkImpl<mode>(log_, element.access_flags, current_database, element.table);
        else
            return checkImpl<mode>(log_, element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            return checkImpl<mode>(log_, element.access_flags, current_database, element.table, element.columns);
        else
            return checkImpl<mode>(log_, element.access_flags, element.database, element.table, element.columns);
    }
}


template <int mode>
bool AccessRightsContext::checkImpl(Poco::Logger * log_, const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!checkImpl<mode>(log_, element))
            return false;
    return true;
}


void AccessRightsContext::check(const AccessFlags & access) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access); }
void AccessRightsContext::check(const AccessFlags & access, const std::string_view & database) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access, database); }
void AccessRightsContext::check(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access, database, table); }
void AccessRightsContext::check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access, database, table, column); }
void AccessRightsContext::check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access, database, table, columns); }
void AccessRightsContext::check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access, database, table, columns); }
void AccessRightsContext::check(const AccessRightsElement & access) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access); }
void AccessRightsContext::check(const AccessRightsElements & access) const { checkImpl<THROW_IF_ACCESS_DENIED>(nullptr, access); }

bool AccessRightsContext::isGranted(const AccessFlags & access) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access, database); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access, database, table); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access, database, table, column); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access, database, table, columns); }
bool AccessRightsContext::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access, database, table, columns); }
bool AccessRightsContext::isGranted(const AccessRightsElement & access) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access); }
bool AccessRightsContext::isGranted(const AccessRightsElements & access) const { return checkImpl<RETURN_FALSE_IF_ACCESS_DENIED>(nullptr, access); }

bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access, database); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access, database, table); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access, database, table, column); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access, database, table, columns); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access, database, table, columns); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessRightsElement & access) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access); }
bool AccessRightsContext::isGranted(Poco::Logger * log_, const AccessRightsElements & access) const { return checkImpl<LOG_WARNING_IF_ACCESS_DENIED>(log_, access); }


boost::shared_ptr<const AccessRights> AccessRightsContext::calculateResultAccess() const
{
    auto res = result_access_cache[0].load();
    if (res)
        return res;
    return calculateResultAccess(readonly, allow_ddl, allow_introspection);
}


boost::shared_ptr<const AccessRights> AccessRightsContext::calculateResultAccess(UInt64 readonly_, bool allow_ddl_, bool allow_introspection_) const
{
    size_t cache_index = static_cast<size_t>(readonly_ != readonly)
                       + static_cast<size_t>(allow_ddl_ != allow_ddl) * 2 +
                       + static_cast<size_t>(allow_introspection_ != allow_introspection) * 3;
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

    result = granted_to_user;

    static const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
        | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
        | AccessType::DETACH_DATABASE | AccessType::DETACH_TABLE | AccessType::DETACH_VIEW | AccessType::TRUNCATE;
    static const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY | AccessType::DETACH_DICTIONARY;
    static const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
    static const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;

    if (readonly_)
        result.fullRevoke(write_table_access | AccessType::SYSTEM);

    if (readonly_ || !allow_ddl_)
        result.fullRevoke(table_and_dictionary_ddl);

    if (readonly_ == 1)
    {
        /// Table functions are forbidden in readonly mode.
        /// For example, for readonly = 2 - allowed.
        result.fullRevoke(AccessType::CREATE_TEMPORARY_TABLE | AccessType::TABLE_FUNCTIONS);
    }

    if (!allow_introspection_)
        result.fullRevoke(AccessType::INTROSPECTION);

    result_access_cache[cache_index].store(result_ptr);
    return std::move(result_ptr);
}

}
