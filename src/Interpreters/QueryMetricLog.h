#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/PeriodicLog.h>
#include <Storages/ColumnsDescription.h>

#include <chrono>
#include <ctime>


namespace DB
{

/** QueryMetricLogElement is a log of query metric values measured at regular time interval.
  */

struct QueryMetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    String query_id{};
    Int64 memory{};
    Int64 background_memory{};
    std::vector<ProfileEvents::Count> profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());

    static std::string name() { return "QueryMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

struct QueryMetricLogStatus
{
    UInt64 interval_milliseconds;
    std::chrono::system_clock::time_point next_collect_time;
    std::vector<ProfileEvents::Count> last_profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    BackgroundSchedulePool::TaskHolder task;
};

class QueryMetricLog : public SystemLog<QueryMetricLogElement>
{
    using SystemLog<QueryMetricLogElement>::SystemLog;
    using TimePoint = std::chrono::system_clock::time_point;
    using Base = SystemLog<QueryMetricLogElement>;

public:
    void shutdown() final;

    void stopCollect();

    // Both startQuery and finishQuery are called from the thread that executes the query
    void startQuery(const String & query_id, TimePoint query_start_time, UInt64 interval_milliseconds);
    void finishQuery(const String & query_id);

private:
    QueryMetricLogElement createLogMetricElement(const String & query_id, std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters, PeriodicLog<QueryMetricLogElement>::TimePoint current_time);

    size_t collect_interval_milliseconds;
    std::mutex queries_mutex;
    std::unordered_map<String, QueryMetricLogStatus> queries;
};

}
