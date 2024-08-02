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
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/PeriodicLog.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

#include <chrono>
#include <mutex>

namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
    extern const Metric MergesMutationsMemoryTracking;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

const auto memory_metrics = std::array{CurrentMetrics::MemoryTracking, CurrentMetrics::MergesMutationsMemoryTracking};

ColumnsDescription QueryMetricLogElement::getColumnsDescription()
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

void QueryMetricLogElement::appendToBlock(MutableColumns & columns) const
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

void QueryMetricLog::stopCollect()
{
    bool old_val = false;
    if (!is_shutdown_metric_thread.compare_exchange_strong(old_val, true))
        return;
    queries_cv.notify_all();
    if (worker_thread)
        worker_thread->join();
}

void QueryMetricLog::startQuery(const String & query_id, TimePoint query_start_time, UInt64 interval_milliseconds)
{
    QueryMetricLogStatus status;
    status.query_id = query_id;
    status.interval_milliseconds = interval_milliseconds;
    status.next_collect_time = query_start_time + std::chrono::milliseconds(interval_milliseconds);

    const auto & profile_events = CurrentThread::getProfileEvents();
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        status.last_profile_events[i] = profile_events[i].load(std::memory_order_relaxed);

    std::lock_guard lock(queries_mutex);
    queries.emplace(std::move(status));

    // Wake up the sleeping thread only if the collection for this query needs to wake up sooner
    const auto & queries_by_next_collect_time = queries.get<ByNextCollectTime>();
    if (query_id == queries_by_next_collect_time.begin()->query_id)
    {
        std::unique_lock cv_lock(queries_cv_mutex);
        queries_cv_wakeup = true;
        cv_lock.unlock();
        queries_cv.notify_all();
    }
}

void QueryMetricLog::finishQuery(const String & query_id)
{
    std::lock_guard lock(queries_mutex);
    auto & queries_by_id = queries.get<ByQueryId>();
    queries_by_id.erase(query_id);
}

void QueryMetricLog::threadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();
    while (!is_shutdown_metric_thread)
    {
        try
        {
            {
                std::lock_guard lock(queries_mutex);
                const auto current_time = std::chrono::system_clock::now();
                if (!queries.empty())
                {
                    auto & queries_by_next_collect_time = queries.get<ByNextCollectTime>();
                    stepFunction(current_time);
                    desired_timepoint = queries_by_next_collect_time.begin()->next_collect_time;
                }
                else
                {
                    // Use an absurdidly far time to avoid waking up too often
                    desired_timepoint = desired_timepoint + std::chrono::hours(1);
                }
            }

            std::unique_lock cv_lock(queries_cv_mutex);
            queries_cv.wait_until(cv_lock, desired_timepoint, [this, desired_timepoint] {
                return queries_cv_wakeup || is_shutdown_metric_thread || desired_timepoint >= std::chrono::system_clock::now();
            });
            queries_cv_wakeup = false;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

QueryMetricLogElement QueryMetricLog::createLogMetricElement(const String & query_id, std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters, PeriodicLog<QueryMetricLogElement>::TimePoint current_time)
{
    auto query_status_it = queries.find(query_id);

    QueryMetricLogElement elem;
    elem.event_time = timeInSeconds(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);
    elem.query_id = query_status_it->query_id;
    elem.memory = CurrentMetrics::values[CurrentMetrics::MemoryTracking];
    elem.background_memory = CurrentMetrics::values[CurrentMetrics::MergesMutationsMemoryTracking];

    // We copy the QueryMetricLogStatus and update the queries in a final step because updating the multi-index set
    // for every profile event doesn't seem a good idea.
    auto new_query_status = *query_status_it;
    new_query_status.next_collect_time += std::chrono::milliseconds(new_query_status.interval_milliseconds);

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const auto & new_value = (*profile_counters)[i];
        elem.profile_events[i] = new_value - new_query_status.last_profile_events[i];
        new_query_status.last_profile_events[i] = new_value;
    }

    queries.modify(query_status_it, [&](QueryMetricLogStatus & query_status) { query_status = std::move(new_query_status); });

    return elem;
}

void QueryMetricLog::stepFunction(TimePoint current_time)
{
    static const auto & process_list = getContext()->getProcessList();

    auto & queries_by_next_collect_time = queries.get<ByNextCollectTime>();
    for (const auto & query_status : queries_by_next_collect_time)
    {
        // The queries are already sorted by next_collect_time, so once we find a query with a next_collect_time
        // in the future, we know we don't need to collect data anymore
        if (query_status.next_collect_time > current_time)
            break;

        const auto query_info = process_list.getQueryInfo(query_status.query_id, false, true, false);

        // The query info should always be found because whenever a query ends, finishQuery is
        // called and the query is removed from the list
        if (!query_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query info not found: {}", query_status.query_id);

        auto elem = createLogMetricElement(query_status.query_id, query_info->profile_counters, current_time);
        add(std::move(elem));
    }
}

}
