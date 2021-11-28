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

using AsynchronousMetricValue = double;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;

/** AsynchronousMetricLog is a log of metric values measured at regular time interval.
  */

struct AsynchronousMetricLogElement
{
    UInt16 event_date;
    time_t event_time;
    Decimal64 event_time_microseconds;
    std::string metric_name;
    double value;

    static std::string name() { return "AsynchronousMetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


}
