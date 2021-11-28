#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <vector>
#include <atomic>
#include <ctime>


namespace DB
{

/** MetricLog is a log of metric values measured at regular time interval.
  */

struct MetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 milliseconds{};

    std::vector<ProfileEvents::Count> profile_events;
    std::vector<CurrentMetrics::Metric> current_metrics;

    static std::string name() { return "MetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

}
