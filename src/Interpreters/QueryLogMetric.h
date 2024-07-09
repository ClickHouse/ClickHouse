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
};

class QueryLogMetric : public PeriodicLog<QueryLogMetricElement>
{
    using PeriodicLog<QueryLogMetricElement>::PeriodicLog;

public:
    void startQuery(String query_id);
    void finishQuery(String query_id);

protected:
    void stepFunction(TimePoint current_time) override;

private:
    std::mutex queries_status_mutex;
    std::unordered_map<String, QueryLogMetricStatus> queries_status;
};

}
