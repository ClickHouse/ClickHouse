#include <base/getFQDNOrHostName.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryLogMetric.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

#include <mutex>
#include <unordered_map>

#include <Common/logger_useful.h>
#include "Interpreters/PeriodicLog.h"


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
    extern const Metric MergesMutationsMemoryTracking;
}

namespace DB
{

const auto memory_metrics = std::array{CurrentMetrics::MemoryTracking, CurrentMetrics::MergesMutationsMemoryTracking};

ColumnsDescription QueryLogMetricElement::getColumnsDescription()
{
    ColumnsDescription result;
    ParserCodec codec_parser;

    result.add({"hostname",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Hostname of the server executing the query."});
    result.add({"event_date",
                std::make_shared<DataTypeDate>(),
                parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Event date."});
    result.add({"event_time",
                std::make_shared<DataTypeDateTime>(),
                parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Event time."});
    result.add({"event_time_microseconds",
                std::make_shared<DataTypeDateTime64>(6),
                "Event time with microseconds resolution."});
    result.add({"query_id",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Query ID."});

    for (const auto & metric : memory_metrics)
    {
        auto name = fmt::format("CurrentMetric_{}", CurrentMetrics::getName(metric));
        const auto * comment = CurrentMetrics::getDocumentation(metric);
        result.add({std::move(name), std::make_shared<DataTypeInt64>(), comment});
    }

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        auto name = fmt::format("ProfileEvent_{}", ProfileEvents::getName(ProfileEvents::Event(i)));
        const auto * comment = ProfileEvents::getDocumentation(ProfileEvents::Event(i));
        result.add({std::move(name), std::make_shared<DataTypeUInt64>(), comment});
    }

    return result;
}

void QueryLogMetricElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(query_id);
    columns[column_idx++]->insert(memory);
    columns[column_idx++]->insert(background_memory);

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);
}

QueryLogMetricElement createLogMetricElement(std::string_view query_id, PeriodicLog<QueryLogMetricElement>::TimePoint current_time, QueryLogMetricStatus & query_status)
{
    QueryLogMetricElement elem;
    elem.event_time = timeInSeconds(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);
    elem.query_id = query_id;

    elem.memory = CurrentMetrics::values[CurrentMetrics::MemoryTracking];
    elem.background_memory = CurrentMetrics::values[CurrentMetrics::MergesMutationsMemoryTracking];

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        elem.profile_events[i] = query_status.last_profile_events[i] - value;
        query_status.last_profile_events[i] = value;
    }

    return elem;
}

void QueryLogMetric::startQuery(String query_id)
{
    QueryLogMetricStatus status;
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        status.last_profile_events[i] = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(queries_status_mutex);
    queries_status.emplace(query_id, std::move(status));
}

void QueryLogMetric::finishQuery(String query_id)
{
    std::lock_guard<std::mutex> lock(queries_status_mutex);
    if (auto it = queries_status.find(query_id); it != queries_status.end())
        queries_status.erase(it);
}

void QueryLogMetric::stepFunction(TimePoint current_time)
{
    static const auto & process_list = context->getProcessList();

    LOG_DEBUG(getLogger("PMO"), "QueryLogMetric::stepFunction");
    const auto & queries = process_list.getInfo(false, true, false);
    std::lock_guard<std::mutex> lock(queries_status_mutex);
    for (const auto & query : queries)
    {
        const auto & query_id = query.client_info.current_query_id;
        const auto it = queries_status.find(query_id);

        // Do not track queries that have not started to be monitored. e.g. internal queries
        if (it == queries_status.end())
            return;

        auto elem = createLogMetricElement(query_id, current_time, it->second);
        add(std::move(elem));
    }
}

}
