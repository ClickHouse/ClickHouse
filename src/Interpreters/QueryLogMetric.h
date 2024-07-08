#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>


namespace DB
{

/** QueryLogMetricElement is a log of query metric values measured at regular time interval.
  */

struct QueryLogMetricElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    String query_id{};
    UInt64 interval_microseconds{};

    std::vector<ProfileEvents::Count> profile_events;
    std::vector<CurrentMetrics::Metric> current_metrics;

    static std::string name() { return "QueryLogMetric"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

struct QueryLogMetricStatus
{
  using QueryTime = std::chrono::time_point<std::chrono::system_clock>;

  QueryTime start_time{};
  QueryTime last_time{};
  UInt64 interval_microseconds;
  std::vector<ProfileEvents::Count> last_profile_events;
};

class QueryLogMetric : public PeriodicLog<QueryLogMetricElement>
{
    using PeriodicLog<QueryLogMetricElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override;
};

}
