#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/AsynchronousMetrics.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/** AsynchronousMetricLog is a log of metric values measured at regular time interval.
  */
struct AsynchronousMetricLogElement
{
    UInt16 event_date{};
    time_t event_time{};
    std::string metric_name;
    /// The key of a key-value metric (e.g. the CPU core number or the block device name). Empty for scalar metrics.
    std::string key;
    double value{};

    static std::string name() { return "AsynchronousMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class AsynchronousMetricLog : public SystemLog<AsynchronousMetricLogElement>
{
public:
    using SystemLog<AsynchronousMetricLogElement>::SystemLog;

    void addValues(const AsynchronousMetricValues &);

    /// This table is usually queried for fixed metric name (and, for key-value metrics, a fixed key).
    static const char * getDefaultOrderBy() { return "metric, key, event_date, event_time"; }
};

}
