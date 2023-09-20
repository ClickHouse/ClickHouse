#include <Interpreters/QueryLog.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/ClickHouseRevision.h>
#include <Common/IPv6ToBinary.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>

#include <Poco/Net/IPAddress.h>

#include <array>


namespace DB
{

NamesAndTypesList QueryLogElement::getNamesAndTypes()
{
    auto query_status_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"QueryStart",                  static_cast<Int8>(QUERY_START)},
            {"QueryFinish",                 static_cast<Int8>(QUERY_FINISH)},
            {"ExceptionBeforeStart",        static_cast<Int8>(EXCEPTION_BEFORE_START)},
            {"ExceptionWhileProcessing",    static_cast<Int8>(EXCEPTION_WHILE_PROCESSING)}
        });

    auto query_cache_usage_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Unknown",     static_cast<Int8>(QueryCache::Usage::Unknown)},
            {"None",        static_cast<Int8>(QueryCache::Usage::None)},
            {"Write",       static_cast<Int8>(QueryCache::Usage::Write)},
            {"Read",        static_cast<Int8>(QueryCache::Usage::Read)}
        });

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_low_cardinality_string = std::make_shared<DataTypeArray>(low_cardinality_string);

    return
    {
        {"type", std::move(query_status_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"query_start_time", std::make_shared<DataTypeDateTime>()},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>()},

        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"written_rows", std::make_shared<DataTypeUInt64>()},
        {"written_bytes", std::make_shared<DataTypeUInt64>()},
        {"result_rows", std::make_shared<DataTypeUInt64>()},
        {"result_bytes", std::make_shared<DataTypeUInt64>()},
        {"memory_usage", std::make_shared<DataTypeUInt64>()},

        {"current_database", low_cardinality_string},
        {"query", std::make_shared<DataTypeString>()},
        {"formatted_query", std::make_shared<DataTypeString>()},
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>()},
        {"query_kind", low_cardinality_string},
        {"databases", array_low_cardinality_string},
        {"tables", array_low_cardinality_string},
        {"columns", array_low_cardinality_string},
        {"partitions", array_low_cardinality_string},
        {"projections", array_low_cardinality_string},
        {"views", array_low_cardinality_string},
        {"exception_code", std::make_shared<DataTypeInt32>()},
        {"exception", std::make_shared<DataTypeString>()},
        {"stack_trace", std::make_shared<DataTypeString>()},

        {"is_initial_query", std::make_shared<DataTypeUInt8>()},
        {"user", low_cardinality_string},
        {"query_id", std::make_shared<DataTypeString>()},
        {"address", DataTypeFactory::instance().get("IPv6")},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"initial_user", low_cardinality_string},
        {"initial_query_id", std::make_shared<DataTypeString>()},
        {"initial_address", DataTypeFactory::instance().get("IPv6")},
        {"initial_port", std::make_shared<DataTypeUInt16>()},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>()},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"interface", std::make_shared<DataTypeUInt8>()},
        {"is_secure", std::make_shared<DataTypeUInt8>()},
        {"os_user", low_cardinality_string},
        {"client_hostname", low_cardinality_string},
        {"client_name", low_cardinality_string},
        {"client_revision", std::make_shared<DataTypeUInt32>()},
        {"client_version_major", std::make_shared<DataTypeUInt32>()},
        {"client_version_minor", std::make_shared<DataTypeUInt32>()},
        {"client_version_patch", std::make_shared<DataTypeUInt32>()},
        {"http_method", std::make_shared<DataTypeUInt8>()},
        {"http_user_agent", low_cardinality_string},
        {"http_referer", std::make_shared<DataTypeString>()},
        {"forwarded_for", std::make_shared<DataTypeString>()},
        {"quota_key", std::make_shared<DataTypeString>()},
        {"distributed_depth", std::make_shared<DataTypeUInt64>()},

        {"revision", std::make_shared<DataTypeUInt32>()},

        {"log_comment", std::make_shared<DataTypeString>()},

        {"thread_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>())},
        {"Settings", std::make_shared<DataTypeMap>(low_cardinality_string, low_cardinality_string)},

        {"used_aggregate_functions", array_low_cardinality_string},
        {"used_aggregate_function_combinators", array_low_cardinality_string},
        {"used_database_engines", array_low_cardinality_string},
        {"used_data_type_families", array_low_cardinality_string},
        {"used_dictionaries", array_low_cardinality_string},
        {"used_formats", array_low_cardinality_string},
        {"used_functions", array_low_cardinality_string},
        {"used_storages", array_low_cardinality_string},
        {"used_table_functions", array_low_cardinality_string},

        {"used_row_policies", array_low_cardinality_string},

        {"transaction_id", getTransactionIDDataType()},

        {"query_cache_usage", std::move(query_cache_usage_datatype)},

        {"asynchronous_read_counters", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>())},
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

    columns[i++]->insert(type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(query_start_time);
    columns[i++]->insert(query_start_time_microseconds);
    columns[i++]->insert(query_duration_ms);

    columns[i++]->insert(read_rows);
    columns[i++]->insert(read_bytes);
    columns[i++]->insert(written_rows);
    columns[i++]->insert(written_bytes);
    columns[i++]->insert(result_rows);
    columns[i++]->insert(result_bytes);

    columns[i++]->insert(memory_usage);

    columns[i++]->insertData(current_database.data(), current_database.size());
    columns[i++]->insertData(query.data(), query.size());
    columns[i++]->insertData(formatted_query.data(), formatted_query.size());
    columns[i++]->insert(normalized_query_hash);

    const std::string_view query_kind_str = magic_enum::enum_name(query_kind);
    columns[i++]->insertData(query_kind_str.data(), query_kind_str.size());

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

    columns[i++]->insert(exception_code);
    columns[i++]->insertData(exception.data(), exception.size());
    columns[i++]->insertData(stack_trace.data(), stack_trace.size());

    appendClientInfo(client_info, columns, i);

    columns[i++]->insert(ClickHouseRevision::getVersionRevision());

    columns[i++]->insertData(log_comment.data(), log_comment.size());

    {
        Array threads_array;
        threads_array.reserve(thread_ids.size());
        for (const UInt64 thread_id : thread_ids)
            threads_array.emplace_back(thread_id);
        columns[i++]->insert(threads_array);
    }

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
        auto & column_row_policies_names = typeid_cast<ColumnArray &>(*columns[i++]);

        auto fill_column = [](const auto & data, ColumnArray & column)
        {
            size_t size = 0;
            for (const auto & value : data)
            {
                column.getData().insert(value);
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
        fill_column(used_row_policies, column_row_policies_names);
    }

    columns[i++]->insert(Tuple{tid.start_csn, tid.local_tid, tid.host_id});

    columns[i++]->insert(query_cache_usage);

    if (async_read_counters)
        async_read_counters->dumpToMapColumn(columns[i++].get());
    else
        columns[i++]->insertDefault();
}

void QueryLogElement::appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i)
{
    columns[i++]->insert(client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY);

    columns[i++]->insert(client_info.current_user);
    columns[i++]->insert(client_info.current_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.current_address.host()).data(), 16);
    columns[i++]->insert(client_info.current_address.port());

    columns[i++]->insert(client_info.initial_user);
    columns[i++]->insert(client_info.initial_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.initial_address.host()).data(), 16);
    columns[i++]->insert(client_info.initial_address.port());
    columns[i++]->insert(client_info.initial_query_start_time);
    columns[i++]->insert(client_info.initial_query_start_time_microseconds);

    columns[i++]->insert(static_cast<UInt64>(client_info.interface));
    columns[i++]->insert(static_cast<UInt64>(client_info.is_secure));

    columns[i++]->insert(client_info.os_user);
    columns[i++]->insert(client_info.client_hostname);
    columns[i++]->insert(client_info.client_name);
    columns[i++]->insert(client_info.client_tcp_protocol_version);
    columns[i++]->insert(client_info.client_version_major);
    columns[i++]->insert(client_info.client_version_minor);
    columns[i++]->insert(client_info.client_version_patch);

    columns[i++]->insert(static_cast<UInt64>(client_info.http_method));
    columns[i++]->insert(client_info.http_user_agent);
    columns[i++]->insert(client_info.http_referer);
    columns[i++]->insert(client_info.forwarded_for);

    columns[i++]->insert(client_info.quota_key);
    columns[i++]->insert(client_info.distributed_depth);
}
}
