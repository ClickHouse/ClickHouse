#include <Interpreters/QueryLog.h>
#include <Common/SystemTableDocumentation.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/AsyncReadCounters.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Interpreters/ProfileEventsExt.h>
#include <base/getFQDNOrHostName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DateLUTImpl.h>
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
        {"type", std::move(query_status_datatype), "Type of an event that occurred when executing the query. Values: `QueryStart` — successful start of query execution, `QueryFinish` — successful end of query execution, `ExceptionBeforeStart` — exception before the start of query execution, `ExceptionWhileProcessing` — exception during the query execution."},
        {"event_date", std::make_shared<DataTypeDate>(), "Query starting date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Query starting time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query starting time with microseconds precision."},
        {"query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of query execution."},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of query execution with microsecond precision."},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of query execution in milliseconds."},

        {"read_rows", std::make_shared<DataTypeUInt64>(), "Total number of rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for IN and JOIN. For distributed queries read_rows includes the total number of rows read at all replicas. Each replica sends it's read_rows value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Total number of bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for IN and JOIN. For distributed queries read_bytes includes the total number of rows read at all replicas. Each replica sends it's read_bytes value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "The number of rows written by the query, including any rows written by downstream inserts triggered by the pipeline, such as attached materialized views. For a synchronous insert these downstream rows are recorded on the `query_kind` = `Insert` entry; for an asynchronous insert they are recorded on the `query_kind` = `AsyncInsertFlush` entry, while the client-facing `Insert` entry records only the rows accepted from the client. For queries that do not write rows, it is 0."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "The number of bytes written by the query (uncompressed), including any bytes written by downstream inserts triggered by the pipeline, such as attached materialized views. For a synchronous insert these downstream bytes are recorded on the `query_kind` = `Insert` entry; for an asynchronous insert they are recorded on the `query_kind` = `AsyncInsertFlush` entry, while the client-facing `Insert` entry records only the bytes accepted from the client. For queries that do not write data, it is 0."},
        {"result_rows", std::make_shared<DataTypeUInt64>(), "Number of rows in the result of a SELECT query, or the number of rows written by an insert. For a synchronous insert this includes rows written by downstream inserts triggered by the pipeline (such as attached materialized views) on the `query_kind` = `Insert` entry; for an asynchronous insert those downstream rows are recorded on the `query_kind` = `AsyncInsertFlush` entry, while the client-facing `Insert` entry records only the rows accepted from the client."},
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

        {"is_initial_query", std::make_shared<DataTypeUInt8>(), "Whether the query is initial. Possible values: 1 — an initial (top-level) query, 0 — a child query initiated by another query, including queries for distributed execution and internal subqueries."},
        {"connection_address", DataTypeFactory::instance().get("IPv6"), "The client IP address from which the connection was made. When connected through a proxy, this will be the address of the proxy."},
        {"connection_port", std::make_shared<DataTypeUInt16>(), "The client port from which the connection was made. When connected through a proxy, this will be the port of the proxy."},
        {"user", low_cardinality_string, "Name of the user who initiated the current query."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"address", DataTypeFactory::instance().get("IPv6"), "IP address that was used to make the query. When connected through a proxy and `auth_use_forwarded_address` is set, this will be the address of the client instead of the proxy."},
        {"port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the query. When connected through a proxy and `auth_use_forwarded_address` is set, this will be the port of the client instead of the proxy."},
        {"initial_user", low_cardinality_string, "Name of the user who ran the initial query in the same query chain."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query in the same query chain."},
        {"initial_address", DataTypeFactory::instance().get("IPv6"), "IP address from which the initial query in the same query chain was launched."},
        {"initial_port", std::make_shared<DataTypeUInt16>(), "Client port from which the initial query in the same query chain was launched."},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of the initial query in the same query chain."},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of the initial query in the same query chain, with microsecond precision."},
        {"authenticated_user", low_cardinality_string, "Name of the user who was authenticated in the session."},
        {"interface", std::make_shared<DataTypeUInt8>(), "Interface that the query was initiated from. Possible values: 1 — TCP, 2 — HTTP."},
        {"is_secure", std::make_shared<DataTypeUInt8>(), "The flag whether a query was executed over a secure interface"},
        {"os_user", low_cardinality_string, "Operating system username who runs clickhouse-client."},
        {"client_hostname", low_cardinality_string, "Hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", low_cardinality_string, "The clickhouse-client or another TCP client name."},
        {"client_agent", low_cardinality_string, "The AI coding agent that invoked the client (e.g. `claude-code`, `cursor`), detected from environment variables. Empty if no agent was detected."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "Major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "Minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},
        {"script_query_number", std::make_shared<DataTypeUInt32>(), "The query number in a script with multiple queries for clickhouse-client."},
        {"script_line_number", std::make_shared<DataTypeUInt32>(), "The line number of the query start in a script with multiple queries for clickhouse-client."},
        {"http_method", std::make_shared<DataTypeUInt8>(), "HTTP method that initiated the query. Possible values: 0 - The query was launched from the TCP interface, 1 - GET method was used, 2 - POST method was used, 4 - PUT method was used, 5 - DELETE method was used, 6 - HEAD method was used."},
        {"http_user_agent", low_cardinality_string, "HTTP header UserAgent passed in the HTTP query."},
        {"http_referer", std::make_shared<DataTypeString>(), "HTTP header Referer passed in the HTTP query (contains an absolute or partial address of the page making the query)."},
        {"forwarded_for", std::make_shared<DataTypeString>(), "HTTP header X-Forwarded-For passed in the HTTP query."},
        {"quota_key", std::make_shared<DataTypeString>(), "The quota key specified in the quotas setting (see keyed)."},
        {"distributed_depth", std::make_shared<DataTypeUInt64>(), "How many times a query was forwarded between servers."},

        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse revision."},

        {"http_handler_name", std::make_shared<DataTypeString>(), "Name of the SQL-defined HTTP handler (CREATE HANDLER) that invoked the query. Empty if the query was not invoked through such a handler."},
        {"http_request_url", std::make_shared<DataTypeString>(), "The HTTP request path (without the query string) that invoked the query. The query string is omitted so that sensitive request parameters are not persisted. Empty for non-HTTP queries."},

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

        {"is_internal", std::make_shared<DataTypeUInt8>(), "Indicates whether it is an auxiliary query executed internally."},
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

    const auto & hostname = getFQDNOrHostName();
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(hostname.data(), hostname.size());
    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(type);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(static_cast<UInt16>(DateLUT::instance().toDayNum(event_time).toUnderType()));
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(event_time));
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(event_time_microseconds);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(query_start_time));
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(query_start_time_microseconds);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(query_duration_ms);

    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(read_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(read_bytes);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(written_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(written_bytes);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(result_rows);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(result_bytes);

    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(memory_usage);

    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(current_database.data(), current_database.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(query.data(), query.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(formatted_query.data(), formatted_query.size());
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(normalized_query_hash);

    const std::string_view query_kind_str = magic_enum::enum_name(query_kind);
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(query_kind_str.data(), query_kind_str.size());

    {
        auto & column_databases = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_tables = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_columns = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_partitions = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_projections = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_views = typeid_cast<ColumnArray &>(*columns[i++]);

        auto fill_column = [](const std::set<String> & data, ColumnArray & column)
        {
            auto & lc_column_data = typeid_cast<ColumnLowCardinality &>(column.getData());

            size_t size = 0;
            for (const auto & name : data)
            {
                lc_column_data.insertData(name.data(), name.size());
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
    typeid_cast<ColumnString &>(*columns[i++]).insertData(exception.data(), exception.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(stack_trace.data(), stack_trace.size());

    appendClientInfo(client_info, columns, i);

    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(ClickHouseRevision::getVersionRevision());

    typeid_cast<ColumnString &>(*columns[i++]).insertData(http_handler_name.data(), http_handler_name.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(http_request_url.data(), http_request_url.size());

    typeid_cast<ColumnString &>(*columns[i++]).insertData(log_comment.data(), log_comment.size());

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
        typeid_cast<ColumnMap &>(*columns[i++]).insertDefault();
    }

    {
        /// Write into the subcolumns directly: IColumn::insert(Field) reaches
        /// ColumnUnique::uniqueInsert(Field), which clones a whole ColumnString per boxed value.
        /// Both key and value are LowCardinality here, so that would be two clones per entry.
        auto & column_map = typeid_cast<ColumnMap &>(*columns[i++]);
        auto & offsets = column_map.getNestedColumn().getOffsets();
        auto & tuple_column = column_map.getNestedData();
        auto & key_column = typeid_cast<ColumnLowCardinality &>(tuple_column.getColumn(0));
        auto & value_column = typeid_cast<ColumnLowCardinality &>(tuple_column.getColumn(1));

        for (const auto & [name, value] : query_settings)
        {
            key_column.insertData(name.data(), name.size());
            value_column.insertData(value.data(), value.size());
        }

        offsets.push_back(offsets.back() + query_settings.size());
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
            auto & lc_column_data = typeid_cast<ColumnLowCardinality &>(column.getData());

            size_t size = 0;
            for (const auto & value : data)
            {
                lc_column_data.insertData(value.data(), value.size());
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

    {
        /// Same as for Settings above: avoid boxing through Field. Only the key is LowCardinality.
        auto & column_map = typeid_cast<ColumnMap &>(*columns[i++]);
        auto & offsets = column_map.getNestedColumn().getOffsets();
        auto & tuple_column = column_map.getNestedData();
        auto & key_column = typeid_cast<ColumnLowCardinality &>(tuple_column.getColumn(0));
        auto & value_column = typeid_cast<ColumnUInt64 &>(tuple_column.getColumn(1));

        for (const auto & [name, value] : async_read_counters)
        {
            key_column.insertData(name.data(), name.size());
            value_column.getData().push_back(value);
        }

        offsets.push_back(offsets.back() + async_read_counters.size());
    }

    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(is_internal);
}

void QueryLogElement::appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i)
{
    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY);

    typeid_cast<ColumnIPv6 &>(*columns[i++]).insertData(IPv6ToBinary(client_info.connection_address->host()).data(), 16);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(client_info.connection_address->port());

    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.current_user.data(), client_info.current_user.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(client_info.current_query_id.data(), client_info.current_query_id.size());
    typeid_cast<ColumnIPv6 &>(*columns[i++]).insertData(IPv6ToBinary(client_info.current_address->host()).data(), 16);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(client_info.current_address->port());

    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.initial_user.data(), client_info.initial_user.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(client_info.initial_query_id.data(), client_info.initial_query_id.size());
    typeid_cast<ColumnIPv6 &>(*columns[i++]).insertData(IPv6ToBinary(client_info.initial_address->host()).data(), 16);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(client_info.initial_address->port());
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(client_info.initial_query_start_time));
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(client_info.initial_query_start_time_microseconds);

    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.authenticated_user.data(), client_info.authenticated_user.size());

    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.interface));
    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.is_secure));

    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.os_user.data(), client_info.os_user.size());
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.getClientHostName().data(), client_info.getClientHostName().size());
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.client_name.data(), client_info.client_name.size());
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.client_agent.data(), client_info.client_agent.size());
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.client_tcp_protocol_version);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(client_info.client_version_major));
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(client_info.client_version_minor));
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(client_info.client_version_patch));

    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.script_query_number);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(client_info.script_line_number);

    typeid_cast<ColumnUInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(client_info.http_method));
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(client_info.http_user_agent.data(), client_info.http_user_agent.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(client_info.http_referer.data(), client_info.http_referer.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(client_info.forwarded_for.data(), client_info.forwarded_for.size());

    typeid_cast<ColumnString &>(*columns[i++]).insertData(client_info.quota_key.data(), client_info.quota_key.size());
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(client_info.distributed_depth);
}
}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "query_log",
    .description = R"DOCS_MD(
Stores metadata and statistics about executed queries, such as start time, duration, error messages, resource usage, and other execution details. It does not store the results of queries.

You can change settings of queries logging in the [query_log](/reference/settings/server-settings/settings/query#query_log) section of the server configuration.

You can disable queries logging by setting [log_queries = 0](/reference/settings/session-settings/log-queries#log_queries). We do not recommend to turn off logging because information in this table is important for solving issues.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_log](/reference/settings/server-settings/settings/query#query_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial (top-level) queries.
2.  Child queries that were initiated by other queries, including queries for distributed execution and internal subqueries such as view evaluation. For these queries, information about the original initial query is shown in the `initial_*` columns.

<Tip>
**Filter initial queries by default**

Generally, add `is_initial_query = 1` whenever you query `system.query_log`. This excludes child queries so individual processing steps are not counted separately from the initial query. This filter does not imply that a query was submitted by a client, because server-internal work can also be an initial query.

Use `initial_query_id` instead when you need to trace an initial query together with child queries that preserve its ID. The initial query has the same value for `initial_query_id` and `query_id`, while child queries in the same chain keep the initial query's `initial_query_id` and have their own `query_id`. Not all work spawned by an initial query is correlated this way: server-dispatched work can start a new initial-query chain with a new `initial_query_id`, as with remote [`QueryRunner`](/reference/engines/table-engines/special/query-runner#cluster-mode) dispatches.

```sql
SELECT
    hostname,
    type,
    query_id,
    initial_query_id,
    is_initial_query,
    query
FROM system.query_log
WHERE initial_query_id = '<query_id_of_initial_query>'
ORDER BY event_time_microseconds;
```

If correlated child queries can run on other nodes, query `system.query_log` on every node, for example with [`clusterAllReplicas`](/reference/functions/table-functions/cluster).
</Tip>

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created.
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created.
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability) setting to reduce the number of queries, registered in the `query_log` table.

You can use the [log_formatted_queries](/reference/settings/session-settings/log#log_formatted_queries) setting to log formatted queries to the `formatted_query` column.
)DOCS_MD",
    .examples = R"DOCS_MD(
**Basic example**

```sql
SELECT *
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
ORDER BY query_start_time DESC
LIMIT 1
FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                              clickhouse.eu-central1.internal
type:                                  QueryFinish
event_date:                            2021-11-03
event_time:                            2021-11-03 16:13:54
event_time_microseconds:               2021-11-03 16:13:54.953024
query_start_time:                      2021-11-03 16:13:54
query_start_time_microseconds:         2021-11-03 16:13:54.952325
query_duration_ms:                     0
read_rows:                             69
read_bytes:                            6187
written_rows:                          0
written_bytes:                         0
result_rows:                           69
result_bytes:                          48256
memory_usage:                          0
current_database:                      default
query:                                 DESCRIBE TABLE system.query_log
formatted_query:
normalized_query_hash:                 8274064835331539124
query_kind:
databases:                             []
tables:                                []
columns:                               []
projections:                           []
views:                                 []
exception_code:                        0
exception:
stack_trace:
is_initial_query:                      1
user:                                  default
query_id:                              7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
address:                               ::ffff:127.0.0.1
port:                                  40452
initial_user:                          default
initial_query_id:                      7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
initial_address:                       ::ffff:127.0.0.1
initial_port:                          40452
initial_query_start_time:              2021-11-03 16:13:54
initial_query_start_time_microseconds: 2021-11-03 16:13:54.952325
interface:                             1
os_user:                               sevirov
client_hostname:                       clickhouse.eu-central1.internal
client_name:                           ClickHouse
client_revision:                       54449
client_version_major:                  21
client_version_minor:                  10
client_version_patch:                  1
http_method:                           0
http_user_agent:
http_referer:
forwarded_for:
quota_key:
revision:                              54456
log_comment:
thread_ids:                            [30776,31174]
ProfileEvents:                         {'Query':1,'NetworkSendElapsedMicroseconds':59,'NetworkSendBytes':2643,'SelectedRows':69,'SelectedBytes':6187,'ContextLock':9,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':817,'UserTimeMicroseconds':427,'SystemTimeMicroseconds':212,'OSCPUVirtualTimeMicroseconds':639,'OSReadChars':894,'OSWriteChars':319}
Settings:                              {'load_balancing':'random','max_memory_usage':'10000000000'}
used_aggregate_functions:              []
used_aggregate_function_combinators:   []
used_database_engines:                 []
used_data_type_families:               []
used_dictionaries:                     []
used_formats:                          []
used_functions:                        []
used_storages:                         []
used_table_functions:                  []
used_executable_user_defined_functions:[]
used_sql_user_defined_functions:       []
used_privileges:                       []
missing_privileges:                    []
query_cache_usage:                     None
```

**Cloud example**

In ClickHouse Cloud, `system.query_log` is local to each node; to see all entries you must query via [`clusterAllReplicas`](/reference/functions/table-functions/cluster).

For example, to aggregate query_log rows from every replica in the “default” cluster you can write:

```sql
SELECT *
FROM clusterAllReplicas('default', system.query_log)
WHERE event_time >= now() - toIntervalHour(1)
LIMIT 10
SETTINGS skip_unavailable_shards = 1;
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.query_thread_log](/reference/system-tables/query_thread_log) — This table contains information about each query execution thread.
)DOCS_MD")

}
