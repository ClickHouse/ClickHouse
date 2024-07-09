#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/PeriodicLog.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>
#include <unordered_map>

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
    std::vector<ProfileEvents::Count> last_profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    UInt64 interval_milliseconds;
    std::chrono::system_clock::time_point next_collect_time;
};

struct CloseQuery
{
    String query_id;
    std::chrono::system_clock::time_point next_collect_time;
};

class QueryLogMetric : public PeriodicLog<QueryLogMetricElement>
{
    using PeriodicLog<QueryLogMetricElement>::PeriodicLog;

public:
    void startQuery(String query_id, TimePoint query_start_time, UInt64 interval_milliseconds);
    void finishQuery(String query_id);

protected:
    void stepFunction(TimePoint current_time) override;

private:
    std::mutex queries_mutex;
    CloseQuery queries_closest;
    std::unordered_map<String, QueryLogMetricStatus> queries;

};

}
