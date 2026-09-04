#include <Interpreters/SessionLog.h>
#include <Interpreters/SessionColumnsCommon.h>

#include <base/getFQDNOrHostName.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Common/DateLUTImpl.h>
#include <Core/Settings.h>
#include <Core/Protocol.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Common/IPv6ToBinary.h>
#include <Access/SettingsProfilesInfo.h>
#include <Interpreters/Context.h>

#include <Poco/Net/SocketAddress.h>

namespace
{
using namespace DB;

auto eventTime()
{
    const auto finish_time = std::chrono::system_clock::now();

    return std::make_pair(timeInSeconds(finish_time), timeInMicroseconds(finish_time));
}

}

namespace DB
{

ColumnsDescription SessionLogElement::getColumnsDescription()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"LoginFailure",           static_cast<Int8>(SESSION_LOGIN_FAILURE)},
            {"LoginSuccess",           static_cast<Int8>(SESSION_LOGIN_SUCCESS)},
            {"Logout",                 static_cast<Int8>(SESSION_LOGOUT)}
        });

    auto identified_with_column = std::make_shared<DataTypeEnum8>(getSessionAuthTypeEnumValues());
    auto interface_type_column = std::make_shared<DataTypeEnum8>(getSessionInterfaceEnumValues());

    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto settings_type_column = getNameValueArrayType(lc_string_datatype, std::make_shared<DataTypeString>());
    auto quotas_type_column = getNameValueArrayType(lc_string_datatype, lc_string_datatype);

    return ColumnsDescription
    {
        {"hostname", lc_string_datatype, "Hostname of the server executing the query."},
        {"type", std::move(event_type), "Login/logout result. Possible values: "
            "LoginFailure — Login error. "
            "LoginSuccess — Successful login. "
            "Logout — Logout from the system."},
        {"auth_id", std::make_shared<DataTypeUUID>(), "Authentication ID, which is a UUID that is automatically generated each time user logins."},
        {"session_id", std::make_shared<DataTypeString>(), "Session ID that is passed by client via HTTP interface."},
        {"event_date", std::make_shared<DataTypeDate>(), "Login/logout date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Login/logout time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Login/logout starting time with microseconds precision."},

        {"user", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User name."},
        {"auth_type", std::make_shared<DataTypeNullable>(std::move(identified_with_column)), "The authentication type."},

        {"roles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of roles to which the profile is applied."},
        {"profiles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of profiles set for all roles and/or users."},
        {"settings", std::move(settings_type_column), "Settings that were changed when the client logged in/out."},
        {"quotas", std::move(quotas_type_column), "Quotas that were bound to this session."},

        {"client_address", DataTypeFactory::instance().get("IPv6"), "The IP address that was used to log in/out."},
        {"client_port", std::make_shared<DataTypeUInt16>(), "The client port that was used to log in/out."},
        {"interface", std::move(interface_type_column), "The interface from which the login was initiated."},
        {"http_user_agent", std::make_shared<DataTypeString>(), "The HTTP User-Agent header, if the session was established over HTTP."},

        {"client_hostname", std::make_shared<DataTypeString>(), "The hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", std::make_shared<DataTypeString>(), "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "The major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "The minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},

        {"failure_reason", std::make_shared<DataTypeString>(), "The exception message containing the reason for the login/logout failure."},

        {"certificate_subjects", std::make_shared<DataTypeArray>(lc_string_datatype),
            "The list of subjects (Common Name and Subject Alternative Names) of the TLS client certificate presented on the connection, in the form 'CN:...' / 'SAN:...'. Empty if no certificate was presented."},
        {"certificate_serial", lc_string_datatype, "Serial number of the TLS client certificate. Empty if no certificate was presented."},
        {"certificate_issuer", lc_string_datatype, "Issuer of the TLS client certificate. Empty if no certificate was presented."},
        /// DateTime64(0) (not DateTime) because X.509 validity times can fall outside the 1970..2106 range
        /// representable by DateTime (UInt32 epoch seconds), e.g. the "no expiration" value 99991231235959Z.
        {"certificate_not_before", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(0, "UTC")),
            "Time from which the TLS client certificate is valid. NULL if no certificate was presented."},
        {"certificate_not_after", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(0, "UTC")),
            "Time after which the TLS client certificate expires. NULL if no certificate was presented."},
    };
}

void SessionLogElement::appendToBlock(MutableColumns & columns) const
{
    chassert(type >= SESSION_LOGIN_FAILURE && type <= SESSION_LOGOUT);
    chassert(
        !user_identified_with
        || (*user_identified_with >= AuthenticationType::NO_PASSWORD && *user_identified_with < AuthenticationType::MAX));

    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(type);
    columns[i++]->insert(auth_id);
    columns[i++]->insert(session_id);
    columns[i++]->insert(static_cast<UInt16>(DateLUT::instance().toDayNum(event_time).toUnderType()));
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    chassert((user && user_identified_with) || client_info.interface == ClientInfo::Interface::TCP_INTERSERVER);
    columns[i++]->insert(user ? Field(*user) : Field());
    columns[i++]->insert(user_identified_with ? Field(*user_identified_with) : Field());

    fillStringArrayColumn(roles, *columns[i++]);
    fillStringArrayColumn(profiles, *columns[i++]);
    fillNameValueArrayColumn(settings, *columns[i++]);
    fillNameValueArrayColumn(quotas, *columns[i++]);

    columns[i++]->insertData(IPv6ToBinary(client_info.current_address->host()).data(), 16);
    columns[i++]->insert(client_info.current_address->port());

    columns[i++]->insert(client_info.interface);
    columns[i++]->insertData(client_info.http_user_agent.data(), client_info.http_user_agent.length());

    columns[i++]->insertData(client_info.getClientHostName().data(), client_info.getClientHostName().length());
    columns[i++]->insertData(client_info.client_name.data(), client_info.client_name.length());
    columns[i++]->insert(client_info.client_tcp_protocol_version);
    columns[i++]->insert(client_info.client_version_major);
    columns[i++]->insert(client_info.client_version_minor);
    columns[i++]->insert(client_info.client_version_patch);

    columns[i++]->insertData(auth_failure_reason.data(), auth_failure_reason.length());

    fillCertificateColumns(certificate_info, columns, i);
}

void SessionLog::addLoginSuccess(const UUID & auth_id,
                                 const String & session_id,
                                 const Settings & settings,
                                 const ContextAccessPtr & access,
                                 const ClientInfo & client_info,
                                 const UserPtr & login_user,
                                 const AuthenticationData & user_authenticated_with,
                                 const std::optional<ClientCertificateInfo> & certificate_info)
{
    add([&](SessionLogElement & log_entry)
    {
        log_entry.auth_id = auth_id;
        log_entry.type = SESSION_LOGIN_SUCCESS;
        std::tie(log_entry.event_time, log_entry.event_time_microseconds) = eventTime();

        log_entry.client_info = client_info;
        log_entry.certificate_info = certificate_info;

        if (login_user)
        {
            log_entry.user = login_user->getName();
            log_entry.user_identified_with = user_authenticated_with.getType();
        }

        log_entry.external_auth_server = user_authenticated_with.getLDAPServerName();

        log_entry.session_id = session_id;

        if (const auto roles_info = access->getRolesInfo())
            log_entry.roles = roles_info->getCurrentRolesNames();

        if (const auto profile_info = access->getDefaultProfileInfo())
            log_entry.profiles = profile_info->getProfileNames();

        SettingsChanges changes = settings.changes();
        for (const auto & change : changes)
            log_entry.settings.emplace_back(change.name, Settings::valueToStringUtil(change.name, change.value));

        for (const auto & quota : access->getQuotaUsages())
            log_entry.quotas.emplace_back(quota.quota_name, quota.quota_key);
    });
}

void SessionLog::addLoginFailure(
        const UUID & auth_id,
        const ClientInfo & info,
        const std::optional<String> & user,
        const Exception & reason,
        const std::optional<ClientCertificateInfo> & certificate_info)
{
    add([&](SessionLogElement & log_entry)
    {
        log_entry.auth_id = auth_id;
        log_entry.type = SESSION_LOGIN_FAILURE;
        std::tie(log_entry.event_time, log_entry.event_time_microseconds) = eventTime();

        log_entry.user = user;
        log_entry.auth_failure_reason = reason.message();
        log_entry.client_info = info;
        log_entry.user_identified_with = AuthenticationType::NO_PASSWORD;
        log_entry.certificate_info = certificate_info;
    });
}

void SessionLog::addLogOut(
    const UUID & auth_id,
    const UserPtr & login_user,
    const AuthenticationData & user_authenticated_with,
    const ClientInfo & client_info,
    const std::optional<ClientCertificateInfo> & certificate_info)
{
    add([&](SessionLogElement & log_entry)
    {
        log_entry.auth_id = auth_id;
        log_entry.type = SESSION_LOGOUT;
        std::tie(log_entry.event_time, log_entry.event_time_microseconds) = eventTime();

        if (login_user)
        {
            log_entry.user = login_user->getName();
            log_entry.user_identified_with = user_authenticated_with.getType();
        }
        log_entry.external_auth_server = user_authenticated_with.getLDAPServerName();
        log_entry.client_info = client_info;
        log_entry.certificate_info = certificate_info;
    });
}

}
