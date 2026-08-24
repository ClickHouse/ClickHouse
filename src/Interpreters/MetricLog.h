#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <vector>
#include <ctime>


namespace DB
{

/** MetricLog is a log of metric values measured at regular time interval.
  */

struct MetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    std::vector<ProfileEvents::Count> profile_events;
    std::vector<CurrentMetrics::Metric> current_metrics;

    static std::string name() { return "MetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class MetricLog : public PeriodicLog<MetricLogElement>
{
    using PeriodicLog<MetricLogElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override;
};

}
