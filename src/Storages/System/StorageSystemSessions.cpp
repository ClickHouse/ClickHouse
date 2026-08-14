#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/IPv6ToBinary.h>

#include <base/getFQDNOrHostName.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SessionColumnsCommon.h>
#include <Interpreters/SessionRegistry.h>

#include <Storages/System/StorageSystemSessions.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Poco/Net/SocketAddress.h>

#include <unordered_map>

namespace DB
{

namespace
{

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

    auto settings_type_column = getNameValueArrayType(lc_string_datatype, std::make_shared<DataTypeString>());
    auto quotas_type_column = getNameValueArrayType(lc_string_datatype, lc_string_datatype);

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
        {"auth_type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getSessionAuthTypeEnumValues())), "The authentication type."},

        {"roles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of roles to which the profile is applied."},
        {"profiles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of profiles set for all roles and/or users."},
        {"settings", std::move(settings_type_column), "Settings that are changed when the client logged in."},
        {"quotas", std::move(quotas_type_column), "Quotas that are bound to this session."},

        {"client_address", DataTypeFactory::instance().get("IPv6"), "The IP address that is used to log in."},
        {"client_port", std::make_shared<DataTypeUInt16>(), "The client port that is used to log in."},
        {"interface", std::make_shared<DataTypeEnum8>(getSessionInterfaceEnumValues()), "The interface from which the login is initiated."},
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

        fillStringArrayColumn(entry->roles, *res_columns[i++]);
        fillStringArrayColumn(entry->profiles, *res_columns[i++]);
        fillNameValueArrayColumn(entry->settings, *res_columns[i++]);

        {
            std::vector<std::pair<String, String>> quota_name_and_key;
            quota_name_and_key.reserve(entry->quotas.size());
            for (const auto & quota : entry->quotas)
                quota_name_and_key.emplace_back(quota.quota_name, quota.quota_key);
            fillNameValueArrayColumn(quota_name_and_key, *res_columns[i++]);
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

        fillCertificateColumns(entry->certificate_info, res_columns, i);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSessions) }
