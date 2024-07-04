#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/QueryLogMetric.h>
#include <base/getFQDNOrHostName.h>
#include "Common/DateLUT.h"
#include <Common/DateLUTImpl.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/CurrentThread.h>

#include <Common/logger_useful.h>

#include <chrono>

namespace DB
{

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
    result.add({"interval_microseconds",
                std::make_shared<DataTypeUInt64>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Time window in microseconds."});

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        auto name = fmt::format("ProfileEvent_{}", ProfileEvents::getName(ProfileEvents::Event(i)));
        const auto * comment = ProfileEvents::getDocumentation(ProfileEvents::Event(i));
        result.add({std::move(name), std::make_shared<DataTypeUInt64>(), comment});
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        auto name = fmt::format("CurrentMetric_{}", CurrentMetrics::getName(CurrentMetrics::Metric(i)));
        const auto * comment = CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i));
        result.add({std::move(name), std::make_shared<DataTypeInt64>(), comment});
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
    columns[column_idx++]->insert(interval_microseconds);

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        columns[column_idx++]->insert(current_metrics[i].toUnderType());
}

void QueryLogMetric::startQueryLogMetric(std::string_view query_id, const QueryTime & time, const UInt64 interval_microseconds)
{
    LOG_DEBUG(getLogger("PMO"), "Start query {}", query_id);
    if (query_id.empty())
        return;

    QueryLogMetricStatus query_status;
    query_status.start_time = time;
    query_status.last_time = time;
    query_status.interval_microseconds = interval_microseconds;
    query_status.last_profile_events.resize(ProfileEvents::end());

    CurrentThread::updatePerformanceCountersIfNeeded();
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        query_status.last_profile_events[i] = value;
    }

    queries.insert({String(query_id), query_status});
}

QueryLogMetricElement createLogMetricElement(std::string_view query_id, const QueryLogMetricStatus::QueryTime & time, QueryLogMetricStatus & query_status)
{
    QueryLogMetricElement elem;
    elem.event_time = timeInSeconds(time);
    elem.event_time_microseconds = timeInMicroseconds(time);
    elem.query_id = query_id;
    elem.interval_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time - query_status.last_time).count();
    elem.profile_events.resize(ProfileEvents::end());

    CurrentThread::updatePerformanceCounters();
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        elem.profile_events[i] = query_status.last_profile_events[i] - value;
        query_status.last_profile_events[i] = value;
    }

    elem.current_metrics.resize(CurrentMetrics::end());
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        elem.current_metrics[i] = CurrentMetrics::values[i];
    }

    query_status.last_time = time;

    return elem;
}

void QueryLogMetric::finishQueryLogMetric(std::string_view query_id, const QueryTime & time)
{
    LOG_DEBUG(getLogger("PMO"), "Finish query {}", query_id);
    if (query_id.empty())
        return;

    auto it = queries.find(String(query_id));
    if (it == queries.end())
        return;

    auto & query_status = it->second;
    const auto elem = createLogMetricElement(query_id, time, query_status);
    add(elem);

    queries.erase(it);
}

void QueryLogMetric::updateQueryLogMetric(std::string_view query_id, const QueryTime & time)
{
    LOG_DEBUG(getLogger("PMO"), "Update query {}", query_id);
    if (query_id.empty())
        return;

    // updateQueryLogMetric is called by the progress callback for all queries.
    // However, only non-internal queries are logged via the startQueryLogMetric/stopQueryLogMetric.
    auto it = queries.find(String(query_id));
    if (it == queries.end())
        return;

    auto & query_status = it->second;
    if (time < query_status.last_time + std::chrono::microseconds(query_status.interval_microseconds))
        return;

    const auto elem = createLogMetricElement(query_id, time, query_status);
    add(elem);
}

}
