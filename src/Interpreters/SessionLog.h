#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>
#include <Access/Common/AuthenticationData.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Columns/IColumn.h>

namespace DB
{

enum SessionLogElementType : int8_t
{
    SESSION_LOGIN_FAILURE = 0,
    SESSION_LOGIN_SUCCESS = 1,
    SESSION_LOGOUT = 2,
};

class ContextAccess;
struct User;
using UserPtr = std::shared_ptr<const User>;

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
    SessionLogElement(const UUID & auth_id_, Type type_);
    SessionLogElement(const SessionLogElement &) = default;
    SessionLogElement & operator=(const SessionLogElement &) = default;
    SessionLogElement(SessionLogElement &&) = default;
    SessionLogElement & operator=(SessionLogElement &&) = default;

    UUID auth_id;

    Type type = SESSION_LOGIN_FAILURE;

    String session_id;
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    std::optional<String> user;
    std::optional<AuthenticationType> user_identified_with;
    String external_auth_server;
    Strings roles;
    Strings profiles;
    std::vector<std::pair<String, String>> settings;

    ClientInfo client_info;
    String auth_failure_reason;

    static std::string name() { return "SessionLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};


/// Instead of typedef - to allow forward declaration.
class SessionLog : public SystemLog<SessionLogElement>
{
    using SystemLog<SessionLogElement>::SystemLog;

public:
    void addLoginSuccess(const UUID & auth_id, std::optional<String> session_id, const Context & login_context, const UserPtr & login_user);
    void addLoginFailure(const UUID & auth_id, const ClientInfo & info, const std::optional<String> & user, const Exception & reason);
    void addLogOut(const UUID & auth_id, const UserPtr & login_user, const ClientInfo & client_info);
};

}
