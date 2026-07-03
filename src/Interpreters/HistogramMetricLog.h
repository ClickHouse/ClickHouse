#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>


namespace DB
{

struct HistogramMetricLogElement
{
    UInt16 event_date{};
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    std::string metric_name;
    Map labels;
    Map histogram;
    UInt64 count{};
    Float64 sum{};

    static std::string name() { return "HistogramMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class HistogramMetricLog : public PeriodicLog<HistogramMetricLogElement>
{
    using PeriodicLog<HistogramMetricLogElement>::PeriodicLog;

public:
    static const char * getDefaultOrderBy() { return "event_date, event_time"; }

protected:
    void stepFunction(TimePoint current_time) override;
};

}
