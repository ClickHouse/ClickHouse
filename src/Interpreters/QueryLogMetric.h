#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/PeriodicLog.h>
#include <Storages/ColumnsDescription.h>

#include <condition_variable>
#include <ctime>
#include <set>

namespace DB
{

/** QueryLogMetricElement is a log of query metric values measured at regular time interval.
  */

struct QueryLogMetricElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    String query_id{};
    Int64 memory{};
    Int64 background_memory{};
    std::vector<ProfileEvents::Count> profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());

    static std::string name() { return "QueryLogMetric"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

struct QueryLogMetricStatus
{
    String query_id;
    UInt64 interval_milliseconds;
    std::chrono::system_clock::time_point next_collect_time;
    std::vector<ProfileEvents::Count> last_profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
};

struct QueryLogMetricsStatusCmp
{
    bool operator()(const QueryLogMetricStatus & lhs, const QueryLogMetricStatus & rhs) const
    {
        return lhs.next_collect_time < rhs.next_collect_time;
    }
};

class QueryLogMetric : public PeriodicLog<QueryLogMetricElement>
{
    using PeriodicLog<QueryLogMetricElement>::PeriodicLog;

public:
    // Both startQuery and finishQuery are called from the thread that executes the query
    void startQuery(const String & query_id, TimePoint query_start_time, UInt64 interval_milliseconds);
    void finishQuery(const String & query_id);

protected:
    void stepFunction(TimePoint current_time) override;
    void threadFunction() override;

private:
    std::mutex queries_mutex;
    std::set<QueryLogMetricStatus, QueryLogMetricsStatusCmp> queries;
    std::mutex queries_cv_mutex;
    std::condition_variable queries_cv;
};

}
