#include <Interpreters/SessionLog.h>

#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Core/Settings.h>
#include <Core/Protocol.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Common/IPv6ToBinary.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Access/SettingsProfilesInfo.h>
#include <Interpreters/Context.h>

#include <cassert>

namespace
{
using namespace DB;

inline DateTime64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline time_t time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

auto eventTime()
{
    const auto finish_time = std::chrono::system_clock::now();

    return std::make_pair(time_in_seconds(finish_time), time_in_microseconds(finish_time));
}

using AuthType = AuthenticationType;
using Interface = ClientInfo::Interface;

void fillColumnArray(const Strings & data, IColumn & column)
{
    auto & array = typeid_cast<ColumnArray &>(column);
    size_t size = 0;
    auto & data_col = array.getData();
    for (const auto & name : data)
    {
        data_col.insertData(name.data(), name.size());
        ++size;
    }
    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + size);
}

}

namespace DB
{

SessionLogElement::SessionLogElement(const UUID & auth_id_, Type type_)
    : auth_id(auth_id_),
      type(type_)
{
    std::tie(event_time, event_time_microseconds) = eventTime();
}

NamesAndTypesList SessionLogElement::getNamesAndTypes()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"LoginFailure",           static_cast<Int8>(SESSION_LOGIN_FAILURE)},
            {"LoginSuccess",           static_cast<Int8>(SESSION_LOGIN_SUCCESS)},
            {"Logout",                 static_cast<Int8>(SESSION_LOGOUT)}
        });

#define AUTH_TYPE_NAME_AND_VALUE(v) std::make_pair(AuthenticationTypeInfo::get(v).raw_name, static_cast<Int8>(v))
    auto identified_with_column = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::PLAINTEXT_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::SHA256_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::DOUBLE_SHA1_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::LDAP),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::KERBEROS),
        });
#undef AUTH_TYPE_NAME_AND_VALUE
    static_assert(static_cast<int>(AuthenticationType::MAX) == 7);

    auto interface_type_column = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"TCP",                    static_cast<Int8>(Interface::TCP)},
            {"HTTP",                   static_cast<Int8>(Interface::HTTP)},
            {"gRPC",                   static_cast<Int8>(Interface::GRPC)},
            {"MySQL",                  static_cast<Int8>(Interface::MYSQL)},
            {"PostgreSQL",             static_cast<Int8>(Interface::POSTGRESQL)},
            {"Local",                  static_cast<Int8>(Interface::LOCAL)},
            {"TCP_Interserver",        static_cast<Int8>(Interface::TCP_INTERSERVER)}
        });
    static_assert(magic_enum::enum_count<Interface>() == 7);

    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto settings_type_column = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes({
                // setting name
                lc_string_datatype,
                // value
                std::make_shared<DataTypeString>()
            })));

    return
    {
        {"type", std::move(event_type)},
        {"auth_id", std::make_shared<DataTypeUUID>()},
        {"session_id", std::make_shared<DataTypeString>()},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"user", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"auth_type", std::make_shared<DataTypeNullable>(std::move(identified_with_column))},

        {"profiles", std::make_shared<DataTypeArray>(lc_string_datatype)},
        {"roles", std::make_shared<DataTypeArray>(lc_string_datatype)},
        {"settings", std::move(settings_type_column)},

        {"client_address", DataTypeFactory::instance().get("IPv6")},
        {"client_port", std::make_shared<DataTypeUInt16>()},
        {"interface", std::move(interface_type_column)},

        {"client_hostname", std::make_shared<DataTypeString>()},
        {"client_name", std::make_shared<DataTypeString>()},
        {"client_revision", std::make_shared<DataTypeUInt32>()},
        {"client_version_major", std::make_shared<DataTypeUInt32>()},
        {"client_version_minor", std::make_shared<DataTypeUInt32>()},
        {"client_version_patch", std::make_shared<DataTypeUInt32>()},

        {"failure_reason", std::make_shared<DataTypeString>()},
    };
}

void SessionLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type >= SESSION_LOGIN_FAILURE && type <= SESSION_LOGOUT);
    assert(user_identified_with >= AuthenticationType::NO_PASSWORD && user_identified_with <= AuthenticationType::MAX);

    size_t i = 0;

    columns[i++]->insert(type);
    columns[i++]->insert(auth_id);
    columns[i++]->insert(session_id);
    columns[i++]->insert(static_cast<DayNum>(DateLUT::instance().toDayNum(event_time).toUnderType()));
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    assert((user && user_identified_with) || client_info.interface == ClientInfo::Interface::TCP_INTERSERVER);
    columns[i++]->insert(user ? Field(*user) : Field());
    columns[i++]->insert(user_identified_with ? Field(*user_identified_with) : Field());

    fillColumnArray(profiles, *columns[i++]);
    fillColumnArray(roles, *columns[i++]);

    {
        auto & settings_array_col = assert_cast<ColumnArray &>(*columns[i++]);
        auto & settings_tuple_col = assert_cast<ColumnTuple &>(settings_array_col.getData());
        auto & names_col = *settings_tuple_col.getColumnPtr(0)->assumeMutable();
        auto & values_col = assert_cast<ColumnString &>(*settings_tuple_col.getColumnPtr(1)->assumeMutable());

        for (const auto & kv : settings)
        {
            names_col.insert(kv.first);
            values_col.insert(kv.second);
        }

        auto & offsets = settings_array_col.getOffsets();
        offsets.push_back(settings_tuple_col.size());
    }

    columns[i++]->insertData(IPv6ToBinary(client_info.current_address.host()).data(), 16);
    columns[i++]->insert(client_info.current_address.port());

    columns[i++]->insert(client_info.interface);

    columns[i++]->insertData(client_info.client_hostname.data(), client_info.client_hostname.length());
    columns[i++]->insertData(client_info.client_name.data(), client_info.client_name.length());
    columns[i++]->insert(client_info.client_tcp_protocol_version);
    columns[i++]->insert(client_info.client_version_major);
    columns[i++]->insert(client_info.client_version_minor);
    columns[i++]->insert(client_info.client_version_patch);

    columns[i++]->insertData(auth_failure_reason.data(), auth_failure_reason.length());
}

void SessionLog::addLoginSuccess(const UUID & auth_id, std::optional<String> session_id, const Context & login_context, const UserPtr & login_user)
{
    const auto access = login_context.getAccess();
    const auto & settings = login_context.getSettingsRef();
    const auto & client_info = login_context.getClientInfo();

    DB::SessionLogElement log_entry(auth_id, SESSION_LOGIN_SUCCESS);
    log_entry.client_info = client_info;

    if (login_user)
    {
        log_entry.user = login_user->getName();
        log_entry.user_identified_with = login_user->auth_data.getType();
    }
    log_entry.external_auth_server = login_user ? login_user->auth_data.getLDAPServerName() : "";

    if (session_id)
        log_entry.session_id = *session_id;

    if (const auto roles_info = access->getRolesInfo())
        log_entry.roles = roles_info->getCurrentRolesNames();

    if (const auto profile_info = access->getDefaultProfileInfo())
        log_entry.profiles = profile_info->getProfileNames();

    for (const auto & s : settings.allChanged())
        log_entry.settings.emplace_back(s.getName(), s.getValueString());

    add(log_entry);
}

void SessionLog::addLoginFailure(
        const UUID & auth_id,
        const ClientInfo & info,
        const std::optional<String> & user,
        const Exception & reason)
{
    SessionLogElement log_entry(auth_id, SESSION_LOGIN_FAILURE);

    log_entry.user = user;
    log_entry.auth_failure_reason = reason.message();
    log_entry.client_info = info;
    log_entry.user_identified_with = AuthenticationType::NO_PASSWORD;

    add(log_entry);
}

void SessionLog::addLogOut(const UUID & auth_id, const UserPtr & login_user, const ClientInfo & client_info)
{
    auto log_entry = SessionLogElement(auth_id, SESSION_LOGOUT);
    if (login_user)
    {
        log_entry.user = login_user->getName();
        log_entry.user_identified_with = login_user->auth_data.getType();
    }
    log_entry.external_auth_server = login_user ? login_user->auth_data.getLDAPServerName() : "";
    log_entry.client_info = client_info;

    add(log_entry);
}

}
