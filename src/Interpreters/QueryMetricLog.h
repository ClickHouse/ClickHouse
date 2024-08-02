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
    String query_id;
    UInt64 interval_milliseconds;
    std::chrono::system_clock::time_point next_collect_time;
    std::vector<ProfileEvents::Count> last_profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());

    bool operator<(const QueryMetricLogStatus & other) const
    {
        return next_collect_time < other.next_collect_time;
    }
};

class QueryMetricLog : public PeriodicLog<QueryMetricLogElement>
{
    using PeriodicLog<QueryMetricLogElement>::PeriodicLog;

public:
    struct ByQueryId{};
    struct ByNextCollectTime{};

    using QuerySet = boost::multi_index_container<
        QueryMetricLogStatus,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::tag<ByQueryId>, boost::multi_index::member<QueryMetricLogStatus, String, &QueryMetricLogStatus::query_id>>,
            boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByNextCollectTime>, boost::multi_index::member<QueryMetricLogStatus, std::chrono::system_clock::time_point, &QueryMetricLogStatus::next_collect_time>>>>;

    void stopCollect() override;

    // Both startQuery and finishQuery are called from the thread that executes the query
    void startQuery(const String & query_id, TimePoint query_start_time, UInt64 interval_milliseconds);
    void finishQuery(const String & query_id);

protected:
    void stepFunction(TimePoint current_time) override;
    void threadFunction() override;

private:
    QueryMetricLogElement createLogMetricElement(const String & query_id, std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters, PeriodicLog<QueryMetricLogElement>::TimePoint current_time);

    std::mutex queries_mutex;
    QuerySet queries;
    std::mutex queries_cv_mutex;
    bool queries_cv_wakeup = false;
    std::condition_variable queries_cv;
};

}
