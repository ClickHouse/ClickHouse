#pragma once

#include <base/defines.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/PeriodicLog.h>
#include <Interpreters/ProcessList.h>
#include <Storages/ColumnsDescription.h>

#include <chrono>
#include <ctime>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

/** QueryMetricLogElement is a log of query metric values measured at regular time interval.
  */

struct QueryMetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    String query_id{};
    UInt64 memory_usage{};
    UInt64 peak_memory_usage{};
    std::vector<ProfileEvents::Count> profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());

    static std::string name() { return "QueryMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

struct QueryMetricLogStatusInfo
{
    UInt64 interval_milliseconds;
    std::chrono::system_clock::time_point last_collect_time;
    std::chrono::system_clock::time_point next_collect_time;
    std::vector<ProfileEvents::Count> last_profile_events = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    BackgroundSchedulePoolTaskHolder task;
};

class QueryMetricLogStatus
{
    using TimePoint = std::chrono::system_clock::time_point;
    using Mutex = std::mutex;

    friend class QueryMetricLog;

public:
    QueryMetricLogStatusInfo info TSA_GUARDED_BY(getMutex());

    UInt64 thread_id TSA_GUARDED_BY(getMutex()) = CurrentThread::get().thread_id;

    /// Return a reference to the mutex, used for Thread Sanitizer annotations.
    Mutex & getMutex() const TSA_RETURN_CAPABILITY(mutex)
    {
        if (!mutex)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutex cannot be NULL");
        return *mutex;
    }

    void scheduleNext(String query_id) TSA_REQUIRES(getMutex());
    std::optional<QueryMetricLogElement> createLogMetricElement(const String & query_id, const QueryStatusInfo & query_info, TimePoint query_info_time, bool schedule_next = true) TSA_REQUIRES(getMutex());

private:
    /// We need to be able to move it for the hash map, so we need to add an indirection here.
    std::shared_ptr<Mutex> mutex = std::make_shared<Mutex>();

    /// finished is guarded by QueryMetricLog's query_mutex, so we should always
    /// access it through QueryMetricLog::[set/get]QueryFinished.
    /// I haven't found a stricter way of adding TSA annotations so that this field
    /// is protected by QueryMetricLog's query mutex.
    bool finished = false;
};

class QueryMetricLog : public SystemLog<QueryMetricLogElement>
{
    using SystemLog<QueryMetricLogElement>::SystemLog;
    using Base = SystemLog<QueryMetricLogElement>;

public:
    using TimePoint = std::chrono::system_clock::time_point;

    void shutdown() final;

    /// Both startQuery and finishQuery are called from the thread that executes the query.
    void startQuery(const String & query_id, TimePoint start_time, UInt64 interval_milliseconds);
    void finishQuery(const String & query_id, TimePoint finish_time, QueryStatusInfoPtr query_info = nullptr);

private:
    void collectMetric(const ProcessList & process_list, String query_id);
    void setQueryFinished(QueryMetricLogStatus & status) TSA_REQUIRES(queries_mutex);
    bool getQueryFinished(QueryMetricLogStatus & status) TSA_REQUIRES(queries_mutex);

    std::mutex queries_mutex;
    std::unordered_map<String, QueryMetricLogStatus> queries TSA_GUARDED_BY(queries_mutex);
};

}
