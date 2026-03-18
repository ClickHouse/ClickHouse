#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>


namespace DB
{

/** AsynchronousMetricLog is a log of metric values measured at regular time interval.
  */
struct AsynchronousMetricLogElement
{
    UInt16 event_date;
    time_t event_time;
    std::string metric_name;
    double value;

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

    /// This table is usually queried for fixed metric name.
    static const char * getDefaultOrderBy() { return "metric, event_date, event_time"; }
};

}
