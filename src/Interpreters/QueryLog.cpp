#include <Interpreters/QueryLog.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/AsyncReadCounters.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <base/getFQDNOrHostName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/IPv6ToBinary.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#include <array>


namespace DB
{

ColumnsDescription QueryLogElement::getColumnsDescription()
{
    auto query_status_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"QueryStart",                  static_cast<Int8>(QUERY_START)},
            {"QueryFinish",                 static_cast<Int8>(QUERY_FINISH)},
            {"ExceptionBeforeStart",        static_cast<Int8>(EXCEPTION_BEFORE_START)},
            {"ExceptionWhileProcessing",    static_cast<Int8>(EXCEPTION_WHILE_PROCESSING)}
        });

    auto query_result_cache_usage_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Unknown",     static_cast<Int8>(QueryResultCacheUsage::Unknown)},
            {"None",        static_cast<Int8>(QueryResultCacheUsage::None)},
            {"Write",       static_cast<Int8>(QueryResultCacheUsage::Write)},
            {"Read",        static_cast<Int8>(QueryResultCacheUsage::Read)}
        });

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_low_cardinality_string = std::make_shared<DataTypeArray>(low_cardinality_string);

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"type", std::move(query_status_datatype), "Type of an event that occurred when executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Query starting date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Query starting time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query starting time with microseconds precision."},
        {"query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of query execution."},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of query execution with microsecond precision."},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of query execution in milliseconds."},

        {"read_rows", std::make_shared<DataTypeUInt64>(), "Total number of rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for IN and JOIN. For distributed queries read_rows includes the total number of rows read at all replicas. Each replica sends it's read_rows value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Total number of bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for IN and JOIN. For distributed queries read_bytes includes the total number of rows read at all replicas. Each replica sends it's read_bytes value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "For INSERT queries, the number of written rows. For other queries, the column value is 0."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "For INSERT queries, the number of written bytes (uncompressed). For other queries, the column value is 0."},
        {"result_rows", std::make_shared<DataTypeUInt64>(), "Number of rows in a result of the SELECT query, or a number of rows in the INSERT query."},
        {"result_bytes", std::make_shared<DataTypeUInt64>(), "RAM volume in bytes used to store a query result."},
        {"memory_usage", std::make_shared<DataTypeUInt64>(), "Memory consumption by the query."},

        {"current_database", low_cardinality_string, "Name of the current database."},
        {"query", std::make_shared<DataTypeString>(), " Query string."},
        {"formatted_query", std::make_shared<DataTypeString>(), "Formatted query string."},
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>(), "A numeric hash value, such as it is identical for queries differ only by values of literals."},
        {"query_kind", low_cardinality_string, "Type of the query."},
        {"databases", array_low_cardinality_string, "Names of the databases present in the query."},
        {"tables", array_low_cardinality_string, "Names of the tables present in the query."},
        {"columns", array_low_cardinality_string, "Names of the columns present in the query."},
        {"partitions", array_low_cardinality_string, "Names of the partitions present in the query."},
        {"projections", array_low_cardinality_string, "Names of the projections used during the query execution."},
        {"views", array_low_cardinality_string, "Names of the (materialized or live) views present in the query."},
        {"exception_code", std::make_shared<DataTypeInt32>(), "Code of an exception."},
        {"exception", std::make_shared<DataTypeString>(), "Exception message."},
        {"stack_trace", std::make_shared<DataTypeString>(), "Stack trace. An empty string, if the query was completed successfully."},

        {"is_initial_query", std::make_shared<DataTypeUInt8>(), "Query type. Possible values: 1 — query was initiated by the client, 0 — query was initiated by another query as part of distributed query execution."},
        {"user", low_cardinality_string, "Name of the user who initiated the current query."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"address", DataTypeFactory::instance().get("IPv6"), "IP address that was used to make the query."},
        {"port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the query."},
        {"initial_user", low_cardinality_string, "Name of the user who ran the initial query (for distributed query execution)."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query (for distributed query execution)."},
        {"initial_address", DataTypeFactory::instance().get("IPv6"), "IP address that the parent query was launched from."},
        {"initial_port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the parent query."},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>(), "Initial query starting time (for distributed query execution)."},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Initial query starting time with microseconds precision (for distributed query execution)."},
        {"authenticated_user", low_cardinality_string, "Name of the user who was authenticated in the session."},
        {"interface", std::make_shared<DataTypeUInt8>(), "Interface that the query was initiated from. Possible values: 1 — TCP, 2 — HTTP."},
        {"is_secure", std::make_shared<DataTypeUInt8>(), "The flag whether a query was executed over a secure interface"},
        {"os_user", low_cardinality_string, "Operating system username who runs clickhouse-client."},
        {"client_hostname", low_cardinality_string, "Hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", low_cardinality_string, "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "Major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "Minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},
        {"script_query_number", std::make_shared<DataTypeUInt32>(), "The query number in a script with multiple queries for clickhouse-client."},
        {"script_line_number", std::make_shared<DataTypeUInt32>(), "The line number of the query start in a script with multiple queries for clickhouse-client."},
        {"http_method", std::make_shared<DataTypeUInt8>(), "HTTP method that initiated the query. Possible values: 0 — The query was launched from the TCP interface, 1 — GET method was used, 2 — POST method was used."},
        {"http_user_agent", low_cardinality_string, "HTTP header UserAgent passed in the HTTP query."},
        {"http_referer", std::make_shared<DataTypeString>(), "HTTP header Referer passed in the HTTP query (contains an absolute or partial address of the page making the query)."},
        {"forwarded_for", std::make_shared<DataTypeString>(), "HTTP header X-Forwarded-For passed in the HTTP query."},
        {"quota_key", std::make_shared<DataTypeString>(), "The quota key specified in the quotas setting (see keyed)."},
        {"distributed_depth", std::make_shared<DataTypeUInt64>(), "How many times a query was forwarded between servers."},

        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse revision."},

        {"log_comment", std::make_shared<DataTypeString>(), "Log comment. It can be set to arbitrary string no longer than max_query_size. An empty string if it is not defined."},

        {"thread_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Thread ids that are participating in query execution. These threads may not have run simultaneously."},
        {"peak_threads_usage", std::make_shared<DataTypeUInt64>(), "Maximum count of simultaneous threads executing the query."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "ProfileEvents that measure different metrics. The description of them could be found in the table system.events"},
        {"Settings", std::make_shared<DataTypeMap>(low_cardinality_string, low_cardinality_string), "Settings that were changed when the client ran the query. To enable logging changes to settings, set the log_query_settings parameter to 1."},

        {"used_aggregate_functions", array_low_cardinality_string, "Canonical names of aggregate functions, which were used during query execution."},
        {"used_aggregate_function_combinators", array_low_cardinality_string, "Canonical names of aggregate functions combinators, which were used during query execution."},
        {"used_database_engines", array_low_cardinality_string, "Canonical names of database engines, which were used during query execution."},
        {"used_data_type_families", array_low_cardinality_string, "Canonical names of data type families, which were used during query execution."},
        {"used_dictionaries", array_low_cardinality_string, "Canonical names of dictionaries, which were used during query execution."},
        {"used_formats", array_low_cardinality_string, "Canonical names of formats, which were used during query execution."},
        {"used_functions", array_low_cardinality_string, "Canonical names of functions, which were used during query execution."},
        {"used_storages", array_low_cardinality_string, "Canonical names of storages, which were used during query execution."},
        {"used_table_functions", array_low_cardinality_string, "Canonical names of table functions, which were used during query execution."},
        {"used_executable_user_defined_functions", array_low_cardinality_string, "Canonical names of executable user defined functions, which were used during query execution."},
        {"used_sql_user_defined_functions", array_low_cardinality_string, "Canonical names of sql user defined functions, which were used during query execution."},

        {"used_row_policies", array_low_cardinality_string, "The list of row policies names that were used during query execution."},

        {"used_privileges", array_low_cardinality_string, "Privileges which were successfully checked during query execution."},
        {"missing_privileges", array_low_cardinality_string, "Privileges that are missing during query execution."},

        {"transaction_id", getTransactionIDDataType(), "The identifier of the transaction in scope of which this query was executed."},

        {"query_cache_usage", std::move(query_result_cache_usage_datatype), "Usage of the query cache during query execution. Values: 'Unknown' = Status unknown, 'None' = The query result was neither written into nor read from the query result cache, 'Write' = The query result was written into the query result cache, 'Read' = The query result was read from the query result cache."},

        {"asynchronous_read_counters", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "Metrics for asynchronous reading."},
    };
}

NamesAndAliases QueryLogElement::getNamesAndAliases()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_low_cardinality_string = std::make_shared<DataTypeArray>(low_cardinality_string);

    return
    {
        {"ProfileEvents.Names", array_low_cardinality_string, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"},
        {"Settings.Names", array_low_cardinality_string, "mapKeys(Settings)" },
        {"Settings.Values", array_low_cardinality_string, "mapValues(Settings)"}
    };
}

void QueryLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insertData(getFQDNOrHostName());
    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(type);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(DateLUT::instance().toDayNum(event_time).toUnderType());
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(event_time);
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(event_time_microseconds);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(query_start_time);
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(query_start_time_microseconds);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(query_duration_ms);

    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(read_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(read_bytes);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(written_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(written_bytes);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(result_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(result_bytes);

    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(memory_usage);

    columns[i++]->insertData(current_database.data(), current_database.size());
    columns[i++]->insertData(query.data(), query.size());
    columns[i++]->insertData(formatted_query.data(), formatted_query.size());
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(normalized_query_hash);

    const std::string_view query_kind_str = magic_enum::enum_name(query_kind);
    columns[i++]->insertData(query_kind_str);

    {
        auto & column_databases = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_tables = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_columns = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_partitions = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_projections = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_views = typeid_cast<ColumnArray &>(*columns[i++]);

        auto fill_column = [](const std::set<String> & data, ColumnArray & column)
        {
            size_t size = 0;
            for (const auto & name : data)
            {
                column.getData().insertData(name.data(), name.size());
                ++size;
            }
            auto & offsets = column.getOffsets();
            offsets.push_back(offsets.back() + size);
        };

        fill_column(query_databases, column_databases);
        fill_column(query_tables, column_tables);
        fill_column(query_columns, column_columns);
        fill_column(query_partitions, column_partitions);
        fill_column(query_projections, column_projections);
        fill_column(query_views, column_views);
    }

    typeid_cast<ColumnInt32 &>(*columns[i++]).getData().push_back(exception_code);
    columns[i++]->insertData(exception.data(), exception.size());
    columns[i++]->insertData(stack_trace.data(), stack_trace.size());

    appendClientInfo(client_info, columns, i);

    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(ClickHouseRevision::getVersionRevision());

    columns[i++]->insertData(log_comment);

    {
        auto & column_thread_ids = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_thread_ids_data = typeid_cast<ColumnUInt64 &>(column_thread_ids.getData());

        for (const UInt64 thread_id : thread_ids)
            column_thread_ids_data.getData().emplace_back(thread_id);

        auto & offsets = column_thread_ids.getOffsets();
        offsets.push_back(offsets.back() + thread_ids.size());
    }

    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(peak_threads_usage);

    if (profile_counters)
    {
        auto * column = columns[i++].get();
        ProfileEvents::dumpToMapColumn(*profile_counters, column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }

    if (query_settings)
    {
        auto * column = columns[i++].get();
        query_settings->dumpToMapColumn(column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }

    {
        auto & column_aggregate_function_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_aggregate_function_combinator_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_database_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_data_type_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_dictionary_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_format_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_function_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_storage_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_table_function_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_executable_user_defined_function_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_sql_user_defined_function_factory_objects = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_row_policies_names = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_used_privileges = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_missing_privileges = typeid_cast<ColumnArray &>(*columns[i++]);

        auto fill_column = [](const auto & data, ColumnArray & column)
        {
            size_t size = 0;
            for (const auto & value : data)
            {
                column.getData().insertData(value);
                ++size;
            }
            auto & offsets = column.getOffsets();
            offsets.push_back(offsets.back() + size);
        };

        fill_column(used_aggregate_functions, column_aggregate_function_factory_objects);
        fill_column(used_aggregate_function_combinators, column_aggregate_function_combinator_factory_objects);
        fill_column(used_database_engines, column_database_factory_objects);
        fill_column(used_data_type_families, column_data_type_factory_objects);
        fill_column(used_dictionaries, column_dictionary_factory_objects);
        fill_column(used_formats, column_format_factory_objects);
        fill_column(used_functions, column_function_factory_objects);
        fill_column(used_storages, column_storage_factory_objects);
        fill_column(used_table_functions, column_table_function_factory_objects);
        fill_column(used_executable_user_defined_functions, column_executable_user_defined_function_factory_objects);
        fill_column(used_sql_user_defined_functions, column_sql_user_defined_function_factory_objects);
        fill_column(used_row_policies, column_row_policies_names);
        fill_column(used_privileges, column_used_privileges);
        fill_column(missing_privileges, column_missing_privileges);
    }

    {
        auto & tid_tuple = typeid_cast<ColumnTuple &>(*columns[i++]);
        typeid_cast<ColumnUInt64 &>(tid_tuple.getColumn(0)).getData().push_back(tid.start_csn);
        typeid_cast<ColumnUInt64 &>(tid_tuple.getColumn(1)).getData().push_back(tid.local_tid);
        typeid_cast<ColumnUUID &>(tid_tuple.getColumn(2)).getData().push_back(tid.host_id);
    }

    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(uint8_t(query_result_cache_usage));

    if (async_read_counters)
        async_read_counters->dumpToMapColumn(columns[i++].get());
    else
        columns[i++]->insertDefault();
}

void QueryLogElement::appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i)
{
    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY);

    columns[i++]->insertData(client_info.current_user);
    columns[i++]->insertData(client_info.current_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.current_address->host()).data(), 16);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(client_info.current_address->port());

    columns[i++]->insertData(client_info.initial_user);
    columns[i++]->insertData(client_info.initial_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.initial_address->host()).data(), 16);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(client_info.initial_address->port());
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.initial_query_start_time);
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(client_info.initial_query_start_time_microseconds);

    columns[i++]->insertData(client_info.authenticated_user);

    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.interface));
    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.is_secure));

    columns[i++]->insertData(client_info.os_user);
    columns[i++]->insertData(client_info.client_hostname);
    columns[i++]->insertData(client_info.client_name);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.client_tcp_protocol_version);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.client_version_major);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.client_version_minor);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.client_version_patch);

    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.script_query_number);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.script_line_number);

    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.http_method));
    columns[i++]->insertData(client_info.http_user_agent);
    columns[i++]->insertData(client_info.http_referer);
    columns[i++]->insertData(client_info.forwarded_for);

    columns[i++]->insertData(client_info.quota_key);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(client_info.distributed_depth);
}
}
