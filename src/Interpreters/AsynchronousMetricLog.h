#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>

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
    std::string metric_name;
    double value;

    static std::string name() { return "AsynchronousMetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;

    /// Returns the list of columns as in CREATE TABLE statement or nullptr.
    /// If it's not nullptr, this list of columns will be used to create the table.
    /// Otherwise the list will be constructed from LogElement::getNamesAndTypes and LogElement::getNamesAndAliases.
    static const char * getCustomColumnList()
    {
        return "event_date Date CODEC(Delta(2), ZSTD(1)), "
               "event_time DateTime CODEC(Delta(4), ZSTD(1)), "
               "metric LowCardinality(String) CODEC(ZSTD(1)), "
               "value Float64 CODEC(ZSTD(3))";
    }
};

class AsynchronousMetricLog : public SystemLog<AsynchronousMetricLogElement>
{
public:
    using SystemLog<AsynchronousMetricLogElement>::SystemLog;

    void addValues(const AsynchronousMetricValues &);

    /// This table is usually queried for fixed metric name.
    static const char * getDefaultOrderBy() { return "(metric, event_date, event_time)"; }
};

}
