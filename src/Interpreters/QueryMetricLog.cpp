#include <base/getFQDNOrHostName.h>
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
#include <fmt/chrono.h>
#include <mutex>


namespace DB
{

static auto logger = getLogger("QueryMetricLog");

ColumnsDescription QueryMetricLogElement::getColumnsDescription()
{
    ColumnsDescription result;
    ParserCodec codec_parser;

    result.add({"query_id",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Query ID."});
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
                parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Event time with microseconds resolution."});
    result.add({"memory_usage",
                std::make_shared<DataTypeUInt64>(),
                "Amount of RAM the query uses. It might not include some types of dedicated memory."});
    result.add({"peak_memory_usage",
                std::make_shared<DataTypeUInt64>(),
                "Maximum amount of RAM the query used."});

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

    columns[column_idx++]->insert(query_id);
    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(memory_usage);
    columns[column_idx++]->insert(peak_memory_usage);

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        columns[column_idx++]->insert(profile_events[i]);
}

void QueryMetricLog::shutdown()
{
    Base::shutdown();
}

void QueryMetricLog::startQuery(const String & query_id, TimePoint start_time, UInt64 interval_milliseconds)
{
    QueryMetricLogStatus status;
    status.interval_milliseconds = interval_milliseconds;
    status.next_collect_time = start_time + std::chrono::milliseconds(interval_milliseconds);

    auto context = getContext();
    const auto & process_list = context->getProcessList();
    status.task = context->getSchedulePool().createTask("QueryMetricLog", [this, &process_list, query_id] {
        auto current_time = std::chrono::system_clock::now();
        const auto query_info = process_list.getQueryInfo(query_id, false, true, false);
        if (!query_info)
        {
            LOG_TRACE(logger, "Query {} is not running anymore, so we couldn't get its QueryStatusInfo", query_id);
            return;
        }

        auto elem = createLogMetricElement(query_id, *query_info, current_time);
        if (elem)
            add(std::move(elem.value()));
    });

    std::lock_guard lock(queries_mutex);
    status.task->scheduleAfter(interval_milliseconds);
    queries.emplace(query_id, std::move(status));
}

void QueryMetricLog::finishQuery(const String & query_id, TimePoint finish_time, QueryStatusInfoPtr query_info)
{
    std::unique_lock lock(queries_mutex);
    auto it = queries.find(query_id);

    /// finishQuery may be called from logExceptionBeforeStart when the query has not even started
    /// yet, so its corresponding startQuery is never called.
    if (it == queries.end())
        return;

    if (query_info)
    {
        auto elem = createLogMetricElement(query_id, *query_info, finish_time, false);
        if (elem)
            add(std::move(elem.value()));
    }

    /// The task has an `exec_mutex` locked while being executed. This same mutex is locked when
    /// deactivating the task, which happens automatically on its destructor. Thus, we cannot
    /// deactivate/destroy the task while it's running. Now, the task locks `queries_mutex` to
    /// prevent concurrent edition of the queries. In short, the mutex order is: exec_mutex ->
    /// queries_mutex. So, to prevent a deadblock we need to make sure that we always lock them in
    /// that order.
    {
        /// Take ownership of the task so that we can destroy it in this scope after unlocking `queries_mutex`.
        auto task = std::move(it->second.task);

        /// Build an empty task for the old task to make sure it does not lock any mutex on its destruction.
        it->second.task = {};

        queries.erase(query_id);

        /// Ensure `queries_mutex` is unlocked before calling task's destructor at the end of this
        /// scope which will lock `exec_mutex`.
        lock.unlock();
    }
}

std::optional<QueryMetricLogElement> QueryMetricLog::createLogMetricElement(const String & query_id, const QueryStatusInfo & query_info, TimePoint query_info_time, bool schedule_next)
{
    /// fmtlib supports subsecond formatting in 10.0.0. We're in 9.1.0, so we need to add the milliseconds ourselves.
    auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(query_info_time);
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(query_info_time - seconds).count();
    LOG_DEBUG(logger, "Collecting query_metric_log for query {} with QueryStatusInfo from {:%Y.%m.%d %H:%M:%S}.{:06}. Schedule next: {}", query_id, seconds, microseconds, schedule_next);

    std::unique_lock lock(queries_mutex);
    auto query_status_it = queries.find(query_id);

    /// The query might have finished while the scheduled task is running.
    if (query_status_it == queries.end())
    {
        lock.unlock();
        LOG_TRACE(logger, "Query {} finished already while this collecting task was running", query_id);
        return {};
    }

    auto & query_status = query_status_it->second;
    if (query_info_time <= query_status.last_collect_time)
    {
        lock.unlock();
        LOG_TRACE(logger, "Query {} has a more recent metrics collected. Skipping this one", query_id);
        return {};
    }

    query_status.last_collect_time = query_info_time;

    QueryMetricLogElement elem;
    elem.event_time = timeInSeconds(query_info_time);
    elem.event_time_microseconds = timeInMicroseconds(query_info_time);
    elem.query_id = query_status_it->first;
    elem.memory_usage = query_info.memory_usage > 0 ? query_info.memory_usage : 0;
    elem.peak_memory_usage = query_info.peak_memory_usage > 0 ? query_info.peak_memory_usage : 0;

    if (query_info.profile_counters)
    {
        for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        {
            const auto & new_value = (*(query_info.profile_counters))[i];
            auto & old_value = query_status.last_profile_events[i];

            /// Profile event counters are supposed to be monotonic. However, at least the `NetworkReceiveBytes` can be inaccurate.
            /// So, since in the future the counter should always have a bigger value than in the past, we skip this event.
            /// It can be reproduced with the following integration tests:
            /// - test_hedged_requests/test.py::test_receive_timeout2
            /// - test_secure_socket::test
            if (new_value < old_value)
                continue;

            elem.profile_events[i] = new_value - old_value;
            old_value = new_value;
        }
    }
    else
    {
        LOG_TRACE(logger, "Query {} has no profile counters", query_id);
        elem.profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    }

    if (schedule_next)
    {
        query_status.next_collect_time += std::chrono::milliseconds(query_status.interval_milliseconds);
        const auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(query_status.next_collect_time - std::chrono::system_clock::now()).count();
        query_status.task->scheduleAfter(wait_time);
    }

    return elem;
}

}
