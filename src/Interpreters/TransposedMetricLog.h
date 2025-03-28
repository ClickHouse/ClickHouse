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

class TransposedMetricLog : public PeriodicLog<TransposedMetricLogElement>
{
    std::string view_name;

public:
    static constexpr auto HOSTNAME_NAME = "hostname";
    static constexpr auto EVENT_DATE_NAME = "event_date";
    static constexpr auto EVENT_TIME_NAME = "event_time";
    static constexpr auto METRIC_NAME = "metric";
    static constexpr auto VALUE_NAME = "value";

    static constexpr std::string_view PROFILE_EVENT_PREFIX = "ProfileEvent_";
    static constexpr std::string_view CURRENT_METRIC_PREFIX = "CurrentMetric_";

    /// This table is usually queried for fixed metric name.
    static const char * getDefaultOrderBy() { return "event_date, event_time, metric"; }

    void prepareTable() override;

    TransposedMetricLog(
        ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<TransposedMetricLogElement>> queue_ = nullptr)
        : PeriodicLog<TransposedMetricLogElement>(context_, settings_, queue_)
        , view_name(settings_.view_name_for_transposed_metric_log)
    {
    }


protected:
    void stepFunction(TimePoint current_time) override;
};

}
