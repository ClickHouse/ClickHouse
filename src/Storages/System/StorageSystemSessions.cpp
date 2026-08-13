#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/IPv6ToBinary.h>
#include <Common/typeid_cast.h>

#include <base/EnumReflection.h>
#include <base/getFQDNOrHostName.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SessionRegistry.h>

#include <Storages/System/StorageSystemSessions.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Poco/Net/SocketAddress.h>

#include <unordered_map>

namespace DB
{

namespace
{

using AuthType = AuthenticationType;
using Interface = ClientInfo::Interface;

DataTypeEnum8::Values getAuthTypeEnumValues()
{
#define AUTH_TYPE_NAME_AND_VALUE(v) std::make_pair(toString(v), static_cast<Int8>(v))
    DataTypeEnum8::Values values
    {
        AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::PLAINTEXT_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SHA256_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::DOUBLE_SHA1_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::LDAP),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::JWT),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::KERBEROS),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SSH_KEY),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SSL_CERTIFICATE),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::BCRYPT_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::HTTP),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SCRAM_SHA256_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_AUTHENTICATION),
    };
#undef AUTH_TYPE_NAME_AND_VALUE
    static_assert(static_cast<int>(AuthenticationType::MAX) == 13);
    return values;
}

DataTypeEnum8::Values getInterfaceEnumValues()
{
    DataTypeEnum8::Values values
    {
        {"TCP",             static_cast<Int8>(Interface::TCP)},
        {"HTTP",            static_cast<Int8>(Interface::HTTP)},
        {"gRPC",            static_cast<Int8>(Interface::GRPC)},
        {"MySQL",           static_cast<Int8>(Interface::MYSQL)},
        {"PostgreSQL",      static_cast<Int8>(Interface::POSTGRESQL)},
        {"Local",           static_cast<Int8>(Interface::LOCAL)},
        {"TCP_Interserver", static_cast<Int8>(Interface::TCP_INTERSERVER)},
        {"Prometheus",      static_cast<Int8>(Interface::PROMETHEUS)},
        {"Background",      static_cast<Int8>(Interface::BACKGROUND)},
        {"ArrowFlight",      static_cast<Int8>(Interface::ARROW_FLIGHT)},
    };
    static_assert(magic_enum::enum_count<Interface>() == 10, "Please update the array above to match the enum.");
    return values;
}

void fillColumnArray(const Strings & data, IColumn & column)
{
    auto & array = typeid_cast<ColumnArray &>(column);
    auto & data_col = array.getData();
    for (const auto & name : data)
        data_col.insertData(name.data(), name.size());
    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + data.size());
}

/// A connection can only have one query in flight at a time, so the current query, if any, can be
/// found by matching the session's client address against the client address of in-flight queries.
std::unordered_map<String, String> getAddressToQueryIdMap(ContextPtr context)
{
    std::unordered_map<String, String> result;
    for (const auto & process : context->getProcessList().getInfo(false, false, false))
        if (process.client_info.current_address)
            result.emplace(process.client_info.current_address->toString(), process.client_info.current_query_id);
    return result;
}

}

ColumnsDescription StorageSystemSessions::getColumnsDescription()
{
    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto settings_type_column = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes({lc_string_datatype, std::make_shared<DataTypeString>()})));

    auto quotas_type_column = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes({lc_string_datatype, lc_string_datatype})));

    return ColumnsDescription
    {
        {"hostname", lc_string_datatype, "Hostname of the server executing the query."},
        {"auth_id", std::make_shared<DataTypeUUID>(), "Authentication ID, which is a UUID that is automatically generated each time user logins."},
        {"session_id", std::make_shared<DataTypeString>(), "Session ID that is passed by client via HTTP interface."},
        {"event_date", std::make_shared<DataTypeDate>(), "Login date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Login time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Login starting time with microseconds precision."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time in seconds since the session is established."},
        {"current_query_id", std::make_shared<DataTypeString>(), "ID of the query currently executing in this session, if any."},

        {"user", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User name."},
        {"auth_type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getAuthTypeEnumValues())), "The authentication type."},

        {"roles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of roles to which the profile is applied."},
        {"profiles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of profiles set for all roles and/or users."},
        {"settings", std::move(settings_type_column), "Settings that are changed when the client logged in."},
        {"quotas", std::move(quotas_type_column), "Quotas that binds to this session."},

        {"client_address", DataTypeFactory::instance().get("IPv6"), "The IP address that is used to log in."},
        {"client_port", std::make_shared<DataTypeUInt16>(), "The client port that is used to log in."},
        {"interface", std::make_shared<DataTypeEnum8>(getInterfaceEnumValues()), "The interface from which the login is initiated."},
        {"http_user_agent", std::make_shared<DataTypeString>(), "The HTTP User-Agent header, if the session is established over HTTP."},
        {"client_hostname", std::make_shared<DataTypeString>(), "The hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", std::make_shared<DataTypeString>(), "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "The major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "The minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},

        {"certificate_subjects", std::make_shared<DataTypeArray>(lc_string_datatype),
            "The list of subjects (Common Name and Subject Alternative Names) of the TLS client certificate presented on the connection, in the form 'CN:...' / 'SAN:...'. Empty if no certificate is presented."},
        {"certificate_serial", lc_string_datatype, "Serial number of the TLS client certificate. Empty if no certificate is presented."},
        {"certificate_issuer", lc_string_datatype, "Issuer of the TLS client certificate. Empty if no certificate is presented."},
        /// DateTime64(0) (not DateTime) because X.509 validity times can fall outside the 1970..2106 range
        /// representable by DateTime (UInt32 epoch seconds), e.g. the "no expiration" value 99991231235959Z.
        {"certificate_not_before", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(0, "UTC")),
            "Time from which the TLS client certificate is valid. NULL if no certificate is presented."},
        {"certificate_not_after", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(0, "UTC")),
            "Time after which the TLS client certificate expires. NULL if no certificate is presented."},
    };
}

void StorageSystemSessions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto address_to_query_id = getAddressToQueryIdMap(context);
    const auto now = std::chrono::system_clock::now();

    for (const auto & entry : context->getSessionRegistry().getEntries())
    {
        size_t i = 0;

        res_columns[i++]->insert(getFQDNOrHostName());
        res_columns[i++]->insert(entry->auth_id);
        res_columns[i++]->insert(entry->session_id);
        res_columns[i++]->insert(static_cast<UInt16>(DateLUT::instance().toDayNum(entry->event_time).toUnderType()));
        res_columns[i++]->insert(entry->event_time);
        res_columns[i++]->insert(entry->event_time_microseconds);
        res_columns[i++]->insert(std::chrono::duration<double>(now - std::chrono::system_clock::from_time_t(entry->event_time)).count());

        String current_query_id;
        if (entry->client_info.current_address)
        {
            auto it = address_to_query_id.find(entry->client_info.current_address->toString());
            if (it != address_to_query_id.end())
                current_query_id = it->second;
        }
        res_columns[i++]->insert(current_query_id);

        res_columns[i++]->insert(entry->user ? Field(*entry->user) : Field());
        res_columns[i++]->insert(entry->auth_type ? Field(*entry->auth_type) : Field());

        fillColumnArray(entry->roles, *res_columns[i++]);
        fillColumnArray(entry->profiles, *res_columns[i++]);

        {
            auto & settings_array_col = assert_cast<ColumnArray &>(*res_columns[i++]);
            auto & settings_tuple_col = assert_cast<ColumnTuple &>(settings_array_col.getData());
            auto & names_col = *settings_tuple_col.getColumnPtr(0)->assumeMutable();
            auto & values_col = assert_cast<ColumnString &>(*settings_tuple_col.getColumnPtr(1)->assumeMutable());

            for (const auto & kv : entry->settings)
            {
                names_col.insert(kv.first);
                values_col.insert(kv.second);
            }

            auto & offsets = settings_array_col.getOffsets();
            offsets.push_back(settings_tuple_col.size());
        }

        {
            auto & quotas_array_col = assert_cast<ColumnArray &>(*res_columns[i++]);
            auto & quotas_tuple_col = assert_cast<ColumnTuple &>(quotas_array_col.getData());
            auto & names_col = *quotas_tuple_col.getColumnPtr(0)->assumeMutable();
            auto & keys_col = *quotas_tuple_col.getColumnPtr(1)->assumeMutable();

            for (const auto & quota : entry->quotas)
            {
                names_col.insert(quota.quota_name);
                keys_col.insert(quota.quota_key);
            }

            auto & offsets = quotas_array_col.getOffsets();
            offsets.push_back(quotas_tuple_col.size());
        }

        if (entry->client_info.current_address)
        {
            res_columns[i++]->insertData(IPv6ToBinary(entry->client_info.current_address->host()).data(), 16);
            res_columns[i++]->insert(entry->client_info.current_address->port());
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(entry->client_info.interface);
        res_columns[i++]->insert(entry->client_info.http_user_agent);
        res_columns[i++]->insert(entry->client_info.client_hostname);
        res_columns[i++]->insert(entry->client_info.client_name);
        res_columns[i++]->insert(entry->client_info.client_tcp_protocol_version);
        res_columns[i++]->insert(entry->client_info.client_version_major);
        res_columns[i++]->insert(entry->client_info.client_version_minor);
        res_columns[i++]->insert(entry->client_info.client_version_patch);

        if (entry->certificate_info)
        {
            fillColumnArray(entry->certificate_info->subjects, *res_columns[i++]);
            res_columns[i++]->insert(entry->certificate_info->serial);
            res_columns[i++]->insert(entry->certificate_info->issuer);
            res_columns[i++]->insert(DecimalField<DateTime64>(entry->certificate_info->not_before, 0));
            res_columns[i++]->insert(DecimalField<DateTime64>(entry->certificate_info->not_after, 0));
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSessions) }
