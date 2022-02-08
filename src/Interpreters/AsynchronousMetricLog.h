#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

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

class AsynchronousMetricLog : public SystemLog<AsynchronousMetricLogElement>
{
public:
    using SystemLog<AsynchronousMetricLogElement>::SystemLog;

    void addValues(const AsynchronousMetricValues &);
};

}
