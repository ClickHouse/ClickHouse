#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>
#include <Access/Authentication.h>

namespace DB
{

enum SessionLogElementType : int8_t
{
    SESSION_LOGIN_FAILURE = 0,
    SESSION_LOGIN_SUCCESS = 1,
    SESSION_LOGOUT = 2,
};

class ContextAccess;

/** A struct which will be inserted as row into session_log table.
  *
  *  Allows to log information about user sessions:
  * - auth attempts, auth result, auth method, etc.
  * - log out events
  */
struct SessionLogElement
{
    using Type = SessionLogElementType;

    SessionLogElement() = default;
    SessionLogElement(const UUID & session_id_, Type type_);
    SessionLogElement(const SessionLogElement &) = default;
    SessionLogElement & operator=(const SessionLogElement &) = default;
    SessionLogElement(SessionLogElement &&) = default;
    SessionLogElement & operator=(SessionLogElement &&) = default;

    UUID session_id;

    Type type = SESSION_LOGIN_FAILURE;

    String session_name;
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    String user;
    Authentication::Type user_identified_with = Authentication::Type::NO_PASSWORD;
    String external_auth_server;
    Strings roles;
    Strings profiles;
    std::vector<std::pair<String, String>> changed_settings;

    ClientInfo client_info;
    String auth_failure_reason;

    static std::string name() { return "SessionLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};


/// Instead of typedef - to allow forward declaration.
class SessionLog : public SystemLog<SessionLogElement>
{
    using SystemLog<SessionLogElement>::SystemLog;

public:
    void addLoginSuccess(const UUID & session_id, std::optional<String> session_name, const Context & login_context);
    void addLoginFailure(const UUID & session_id, const ClientInfo & info, const String & user, const Exception & reason);
    void addLogOut(const UUID & session_id, const String & user, const ClientInfo & client_info);
};

}
