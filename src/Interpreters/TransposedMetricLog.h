#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>


namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

struct TransposedMetricLogElement
{
    UInt16 event_date;
    time_t event_time;
    Decimal64 event_time_microseconds{};
    std::string metric_name;
    Int64 value;
    UInt8 is_event;

    static std::string name() { return "TransposedMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;

};

/// Transposed version of system.metric_log
class TransposedMetricLog : public PeriodicLog<TransposedMetricLogElement>
{
public:
    /// This table is usually queried by time range + some fixed metric name.
    static const char * getDefaultOrderBy() { return "event_date, toStartOfHour(event_time), metric"; }

    static constexpr auto DESCRIPTION = R"(
        Contains history of metrics values from tables system.metrics and system.events.
        Periodically flushed to disk. Transposed form of system.metric_log.)";

    TransposedMetricLog(
        ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<TransposedMetricLogElement>> queue_ = nullptr)
        : PeriodicLog<TransposedMetricLogElement>(context_, settings_, queue_)
    {
    }

protected:
    void stepFunction(TimePoint current_time) override;

private:
    /// stepFunction and flushBufferToLog may be executed concurrently, hence the mutex
    std::vector<ProfileEvents::Count> previous_profile_events TSA_GUARDED_BY(previous_profile_events_mutex) = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    mutable std::mutex previous_profile_events_mutex;
};

}
