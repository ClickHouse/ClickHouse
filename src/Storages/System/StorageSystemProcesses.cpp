#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ProcessList.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/typeid_cast.h>
#include <Common/IPv6ToBinary.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>


namespace DB
{

ColumnsDescription StorageSystemProcesses::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"is_initial_query", std::make_shared<DataTypeUInt8>(), "Whether this query comes directly from user or was issues by ClickHouse server in a scope of distributed query execution."},

        {"user", std::make_shared<DataTypeString>(), "The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the default user. The field contains the username for a specific query, not for a query that this query initiated."},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID, if defined."},
        {"address", DataTypeFactory::instance().get("IPv6"), "The IP address the query was made from. The same for distributed processing. To track where a distributed query was originally made from, look at system.processes on the query requestor server."},
        {"port", std::make_shared<DataTypeUInt16>(), "The client port the query was made from."},

        {"initial_user", std::make_shared<DataTypeString>(), "Name of the user who ran the initial query (for distributed query execution)."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query (for distributed query execution)."},
        {"initial_address", DataTypeFactory::instance().get("IPv6"), "IP address that the parent query was launched from."},
        {"initial_port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the parent query."},

        {"interface", std::make_shared<DataTypeUInt8>(), "The interface which was used to send the query. TCP = 1, HTTP = 2, GRPC = 3, MYSQL = 4, POSTGRESQL = 5, LOCAL = 6, TCP_INTERSERVER = 7."},

        {"os_user", std::make_shared<DataTypeString>(), "Operating system username who runs clickhouse-client."},
        {"client_hostname", std::make_shared<DataTypeString>(), "Hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", std::make_shared<DataTypeString>(), "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt64>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt64>(), "Major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt64>(), "Minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt64>(), "Patch component of the clickhouse-client or another TCP client version."},

        {"http_method", std::make_shared<DataTypeUInt8>(), "HTTP method that initiated the query. Possible values: 0 — The query was launched from the TCP interface. 1 — GET method was used. 2 — POST method was used."},
        {"http_user_agent", std::make_shared<DataTypeString>(), "HTTP header UserAgent passed in the HTTP query."},
        {"http_referer", std::make_shared<DataTypeString>(), "HTTP header Referer passed in the HTTP query (contains an absolute or partial address of the page making the query)."},
        {"forwarded_for", std::make_shared<DataTypeString>(), "HTTP header X-Forwarded-For passed in the HTTP query."},

        {"quota_key", std::make_shared<DataTypeString>(), "The quota key specified in the quotas setting (see keyed)."},
        {"distributed_depth", std::make_shared<DataTypeUInt64>(), "The number of times query was retransmitted between server nodes internally."},

        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time in seconds since request execution started."},
        {"is_cancelled", std::make_shared<DataTypeUInt8>(), "Was query cancelled."},
        {"is_all_data_sent", std::make_shared<DataTypeUInt8>(), "Was all data sent to the client (in other words query had been finished on the server)."},
        {"read_rows", std::make_shared<DataTypeUInt64>(), "The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers."},
        {"total_rows_approx", std::make_shared<DataTypeUInt64>(), "The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "The amount of rows written to the storage."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "The amount of bytes written to the storage."},
        {"memory_usage", std::make_shared<DataTypeInt64>(), "Amount of RAM the query uses. It might not include some types of dedicated memory"},
        {"peak_memory_usage", std::make_shared<DataTypeInt64>(), "The current peak of memory usage."},
        {"query", std::make_shared<DataTypeString>(), "The query text. For INSERT, it does not include the data to insert."},
        {"query_kind", std::make_shared<DataTypeString>(), "The type of the query - SELECT, INSERT, etc."},

        {"thread_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "The list of identificators of all threads which executed this query."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>()), "ProfileEvents calculated for this query."},
        {"Settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "The list of modified user-level settings."},

        {"current_database", std::make_shared<DataTypeString>(), "The name of the current database."},
    };

    description.setAliases({
        {"ProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"},
        {"Settings.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(Settings)" },
        {"Settings.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapValues(Settings)"}
    });

    return description;
}

void StorageSystemProcesses::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    ProcessList::Info info = context->getProcessList().getInfo(true, true, true);

    for (const auto & process : info)
    {
        size_t i = 0;

        res_columns[i++]->insert(process.client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY);

        res_columns[i++]->insert(process.client_info.current_user);
        res_columns[i++]->insert(process.client_info.current_query_id);
        res_columns[i++]->insertData(IPv6ToBinary(process.client_info.current_address.host()).data(), 16);
        res_columns[i++]->insert(process.client_info.current_address.port());

        res_columns[i++]->insert(process.client_info.initial_user);
        res_columns[i++]->insert(process.client_info.initial_query_id);
        res_columns[i++]->insertData(IPv6ToBinary(process.client_info.initial_address.host()).data(), 16);
        res_columns[i++]->insert(process.client_info.initial_address.port());

        res_columns[i++]->insert(UInt64(process.client_info.interface));

        res_columns[i++]->insert(process.client_info.os_user);
        res_columns[i++]->insert(process.client_info.client_hostname);
        res_columns[i++]->insert(process.client_info.client_name);
        res_columns[i++]->insert(process.client_info.client_tcp_protocol_version);
        res_columns[i++]->insert(process.client_info.client_version_major);
        res_columns[i++]->insert(process.client_info.client_version_minor);
        res_columns[i++]->insert(process.client_info.client_version_patch);

        res_columns[i++]->insert(UInt64(process.client_info.http_method));
        res_columns[i++]->insert(process.client_info.http_user_agent);
        res_columns[i++]->insert(process.client_info.http_referer);
        res_columns[i++]->insert(process.client_info.forwarded_for);

        res_columns[i++]->insert(process.client_info.quota_key);
        res_columns[i++]->insert(process.client_info.distributed_depth);

        res_columns[i++]->insert(static_cast<double>(process.elapsed_microseconds) / 1'000'000.0);
        res_columns[i++]->insert(process.is_cancelled);
        res_columns[i++]->insert(process.is_all_data_sent);
        res_columns[i++]->insert(process.read_rows);
        res_columns[i++]->insert(process.read_bytes);
        res_columns[i++]->insert(process.total_rows);
        res_columns[i++]->insert(process.written_rows);
        res_columns[i++]->insert(process.written_bytes);
        res_columns[i++]->insert(process.memory_usage);
        res_columns[i++]->insert(process.peak_memory_usage);
        res_columns[i++]->insert(process.query);
        res_columns[i++]->insert(magic_enum::enum_name(process.query_kind));

        {
            Array threads_array;
            threads_array.reserve(process.thread_ids.size());
            for (const UInt64 thread_id : process.thread_ids)
                threads_array.emplace_back(thread_id);
            res_columns[i++]->insert(threads_array);
        }

        {
            IColumn * column = res_columns[i++].get();

            if (process.profile_counters)
                ProfileEvents::dumpToMapColumn(*process.profile_counters, column, true);
            else
            {
                column->insertDefault();
            }
        }

        {
            IColumn * column = res_columns[i++].get();

            if (process.query_settings)
                process.query_settings->dumpToMapColumn(column, true);
            else
            {
                column->insertDefault();
            }
        }

        res_columns[i++]->insert(process.current_database);
    }
}

}
