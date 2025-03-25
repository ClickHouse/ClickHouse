#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
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
    std::string metric_name;
    Int64 value;

    static std::string name() { return "TransposedMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class TransposedMetricLog : public PeriodicLog<TransposedMetricLogElement>
{
    using PeriodicLog<TransposedMetricLogElement>::PeriodicLog;
public:
    /// This table is usually queried for fixed metric name.
    static const char * getDefaultOrderBy() { return "metric, event_date, event_time"; }
protected:
    void stepFunction(TimePoint current_time) override;
};

}
