#include <base/getFQDNOrHostName.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/logger_useful.h>
#include <Common/UniqueLock.h>
#include <Core/BackgroundSchedulePool.h>
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


namespace DB
{

static auto logger = getLogger("QueryMetricLog");

String timePointToString(QueryMetricLog::TimePoint time)
{
    /// fmtlib supports subsecond formatting in 10.0.0. We're in 9.1.0, so we need to add the milliseconds ourselves.
    auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(time);
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(time - seconds).count();

    return fmt::format("{:%Y.%m.%d %H:%M:%S}.{:06}", seconds, microseconds);
}

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

void QueryMetricLog::collectMetric(const ProcessList & process_list, String query_id)
{
    auto current_time = std::chrono::system_clock::now();
    const auto query_info = process_list.getQueryInfo(query_id, false, true, false);
    if (!query_info)
    {
        LOG_TEST(logger, "Query {} is not running anymore, so we couldn't get its QueryStatusInfo", query_id);
        return;
    }

    UniqueLock global_lock(queries_mutex);
    auto it = queries.find(query_id);

    /// The query might have finished while the scheduled task is running.
    if (it == queries.end())
    {
        global_lock.unlock();
        LOG_TEST(logger, "Query {} not found in the list. Finished while this collecting task was running", query_id);
        return;
    }

    auto & query_status = it->second;
    UniqueLock query_lock(query_status.getMutex());

    /// query_status.finished needs to be written/read while holding the global_lock because there's
    /// a data race between collectMetric and finishQuery. Due to the order in which we need to
    /// unlock mutexes to ensure there's no mutex lock-order-inversion, after getting the
    /// query_status.mutex we need this extra check to ensure the query is still alive.
    if (getQueryFinished(query_status))
    {
        LOG_TEST(logger, "Query {} finished while this collecting task was running", query_id);
        return;
    }
    global_lock.unlock();

    auto elem = query_status.createLogMetricElement(query_id, *query_info, current_time);
    if (elem)
        add(std::move(elem.value()));
}

/// We use TSA_NO_THREAD_SAFETY_ANALYSIS to prevent TSA complaining that we're modifying the query_status fields
/// without locking the mutex. Since we're building it from scratch, there's no harm in not holding it.
/// If we locked it to make TSA happy, TSAN build would falsely complain about `lock-order-inversion (potential deadlock)`
/// which is not a real issue since QueryMetricLogStatus's mutex cannot be locked by anything else
/// until we add it to the queries map.
void QueryMetricLog::startQuery(const String & query_id, TimePoint start_time, UInt64 interval_milliseconds) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    QueryMetricLogStatus query_status;
    QueryMetricLogStatusInfo & info = query_status.info;
    info.interval_milliseconds = interval_milliseconds;
    info.next_collect_time = start_time;

    auto context = getContext();
    const auto & process_list = context->getProcessList();
    info.task = context->getSchedulePool().createTask("QueryMetricLog", [this, &process_list, query_id] {
        collectMetric(process_list, query_id);
    });

    UniqueLock global_lock(queries_mutex);
    query_status.scheduleNext(query_id);
    queries.emplace(query_id, std::move(query_status));
}

void QueryMetricLog::finishQuery(const String & query_id, TimePoint finish_time, QueryStatusInfoPtr query_info)
{
    UniqueLock global_lock(queries_mutex);
    auto it = queries.find(query_id);

    /// finishQuery may be called from logExceptionBeforeStart when the query has not even started
    /// yet, so its corresponding startQuery is never called.
    if (it == queries.end())
        return;

    auto & query_status = it->second;

    /// Get a refcounted reference to the mutex to ensure it's not destroyed until the query is
    /// removed from queries. Otherwise, query_lock would attempt to unlock a non-existing mutex.
    auto mutex = query_status.mutex;
    UniqueLock query_lock(query_status.getMutex());

    /// finishQuery may be called twice for the same query_id if a new query with the same query_id
    /// is attempted to run in case replace_running_query=0 (the default). Make sure we only execute
    /// the finishQuery once.
    auto thread_id = CurrentThread::get().thread_id;
    if (thread_id != query_status.thread_id || query_status.finished)
    {
        LOG_TEST(logger, "Query {} finished from a different thread_id than the one it started it: "
                         "original was {}, this one is {}. Ignoring this finishQuery",
                         query_id, query_status.thread_id, thread_id);
        return;
    }

    setQueryFinished(query_status);
    global_lock.unlock();

    if (query_info)
    {
        auto elem = query_status.createLogMetricElement(query_id, *query_info, finish_time, false);
        if (elem)
            add(std::move(elem.value()));
    }

    /// The task has an `exec_mutex` locked while being executed. This same mutex is locked when
    /// deactivating the task, which happens automatically on its destructor. Thus, we cannot
    /// deactivate/destroy the task while it's running. Now, the task locks `queries_mutex` to
    /// prevent concurrent edition of the `queries`. In short, the mutex order is: `exec_mutex` ->
    /// `queries_mutex` -> `query_status.mutex`. So, to prevent a deadlock we need to make sure that
    /// we always lock them in that order.
    {
        /// Take ownership of the task so that we can destroy it in this scope after unlocking `queries_mutex`.
        auto task = std::move(query_status.info.task);

        /// Build an empty task for the old task to make sure it does not lock any mutex on its destruction.
        query_status.info.task = {};

        /// Unlock `query_status.mutex` before locking `queries_mutex` to prevent lock-order-inversion.
        query_lock.unlock();

        global_lock.lock();
        queries.erase(query_id);

        /// Ensure `queries_mutex` is unlocked before calling task's destructor at the end of this
        /// scope which will lock `exec_mutex`.
        global_lock.unlock();
    }
}

void QueryMetricLog::setQueryFinished(QueryMetricLogStatus & status)
{
    status.finished = true;
}

bool QueryMetricLog::getQueryFinished(QueryMetricLogStatus & status)
{
    return status.finished;
}

void QueryMetricLogStatus::scheduleNext(String query_id)
{
    info.next_collect_time += std::chrono::milliseconds(info.interval_milliseconds);
    const auto now = std::chrono::system_clock::now();
    if (info.next_collect_time > now)
    {
        const auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(info.next_collect_time - now).count();
        LOG_TEST(logger, "Scheduling next collecting task for query_id {} in {} ms", query_id, wait_time);
        info.task->scheduleAfter(wait_time);
    }
    else
    {
        LOG_TEST(logger, "The next collecting task for query {} should have already run at {}. Scheduling it right now",
            query_id, timePointToString(info.next_collect_time));

        /// Skipping lost runs
        info.next_collect_time = now;
        info.task->schedule();
    }
}

std::optional<QueryMetricLogElement> QueryMetricLogStatus::createLogMetricElement(const String & query_id, const QueryStatusInfo & query_info, TimePoint query_info_time, bool schedule_next)
{
    LOG_TEST(logger, "Collecting query_metric_log for query {} and interval {} ms with QueryStatusInfo from {}. Next collection time: {}",
        query_id, info.interval_milliseconds, timePointToString(query_info_time),
        schedule_next ? timePointToString(info.next_collect_time + std::chrono::milliseconds(info.interval_milliseconds)) : "finished");

    if (query_info_time <= info.last_collect_time)
    {
        LOG_TEST(logger, "Query {} has a more recent metrics collected at {}. This metrics are from {}. Skipping this one",
            timePointToString(info.last_collect_time), timePointToString(query_info_time), query_id);
        return {};
    }

    info.last_collect_time = query_info_time;

    QueryMetricLogElement elem;
    elem.event_time = timeInSeconds(query_info_time);
    elem.event_time_microseconds = timeInMicroseconds(query_info_time);
    elem.query_id = query_id;
    elem.memory_usage = query_info.memory_usage > 0 ? query_info.memory_usage : 0;
    elem.peak_memory_usage = query_info.peak_memory_usage > 0 ? query_info.peak_memory_usage : 0;

    if (query_info.profile_counters)
    {
        for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        {
            const auto & new_value = (*(query_info.profile_counters))[i];
            auto & old_value = info.last_profile_events[i];

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
        LOG_TEST(logger, "Query {} has no profile counters", query_id);
        elem.profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    }

    if (schedule_next)
        scheduleNext(query_id);

    return elem;
}

}
