#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <vector>
#include <atomic>
#include <ctime>


namespace DB
{

typedef double AsynchronousMetricValue;
typedef std::unordered_map<std::string, AsynchronousMetricValue> AsynchronousMetricValues;

/** AsynchronousMetricLog is a log of metric values measured at regular time interval.
  */

struct AsynchronousMetricLogElement
{
    UInt16 event_date;
    time_t event_time;
    std::string metric_name;
    double value;

    static std::string name() { return "AsynchronousMetricLog"; }
    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};

class AsynchronousMetricLog : public SystemLog<AsynchronousMetricLogElement>
{
public:
    using SystemLog<AsynchronousMetricLogElement>::SystemLog;

    void addValues(const AsynchronousMetricValues &);
};

}
