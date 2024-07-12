#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/PeriodicLog.h>
#include <Storages/ColumnsDescription.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include <chrono>
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

    bool operator<(const QueryLogMetricStatus & other) const
    {
        return next_collect_time < other.next_collect_time;
    }
};

class QueryLogMetric : public PeriodicLog<QueryLogMetricElement>
{
    using PeriodicLog<QueryLogMetricElement>::PeriodicLog;

public:
    using QuerySet = boost::multi_index_container<
        QueryLogMetricStatus,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::member<QueryLogMetricStatus, String, &QueryLogMetricStatus::query_id>>,
            boost::multi_index::ordered_non_unique<boost::multi_index::member<QueryLogMetricStatus, std::chrono::system_clock::time_point, &QueryLogMetricStatus::next_collect_time>>>>;

    // Both startQuery and finishQuery are called from the thread that executes the query
    void startQuery(const String & query_id, TimePoint query_start_time, UInt64 interval_milliseconds);
    void finishQuery(const String & query_id);

protected:
    void stepFunction(TimePoint current_time) override;
    void threadFunction() override;

private:
    QueryLogMetricElement createLogMetricElement(const String & query_id, std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters, PeriodicLog<QueryLogMetricElement>::TimePoint current_time);

    std::mutex queries_mutex;
    QuerySet queries;
    std::mutex queries_cv_mutex;
    std::condition_variable queries_cv;
};

}
