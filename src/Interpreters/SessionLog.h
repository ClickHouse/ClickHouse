#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientCertificateInfo.h>
#include <Interpreters/ClientInfo.h>
#include <Access/Common/AuthenticationType.h>
#include <Core/NamesAndAliases.h>
#include <Columns/IColumn_fwd.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct Settings;

enum SessionLogElementType : int8_t
{
    SESSION_LOGIN_FAILURE = 0,
    SESSION_LOGIN_SUCCESS = 1,
    SESSION_LOGOUT = 2,
};

class ContextAccess;
struct User;
using UserPtr = std::shared_ptr<const User>;
using ContextAccessPtr = std::shared_ptr<const ContextAccess>;
class AuthenticationData;

/** A struct which will be inserted as row into session_log table.
  *
  *  Allows to log information about user sessions:
  * - auth attempts, auth result, auth method, etc.
  * - log out events
  */
struct SessionLogElement
{
    using Type = SessionLogElementType;

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

    /// TLS client certificate presented on this connection (empty if none was presented).
    bool has_certificate = false;
    Strings certificate_subjects;
    String certificate_serial;
    String certificate_issuer;
    time_t certificate_not_before{};
    time_t certificate_not_after{};

    static std::string name() { return "SessionLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};


/// Instead of typedef - to allow forward declaration.
class SessionLog : public SystemLog<SessionLogElement>
{
    using SystemLog<SessionLogElement>::SystemLog;
public:
    void addLoginSuccess(const UUID & auth_id,
                         const String & session_id,
                         const Settings & settings,
                         const ContextAccessPtr & access,
                         const ClientInfo & client_info,
                         const UserPtr & login_user,
                         const AuthenticationData & user_authenticated_with,
                         const std::optional<ClientCertificateInfo> & certificate_info = {});

    void addLoginFailure(
        const UUID & auth_id,
        const ClientInfo & info,
        const std::optional<String> & user,
        const Exception & reason,
        const std::optional<ClientCertificateInfo> & certificate_info = {});
    void addLogOut(
        const UUID & auth_id,
        const UserPtr & login_user,
        const AuthenticationData & user_authenticated_with,
        const ClientInfo & client_info,
        const std::optional<ClientCertificateInfo> & certificate_info = {});
};

}
