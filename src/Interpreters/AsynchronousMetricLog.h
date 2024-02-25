#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <vector>
#include <atomic>
#include <ctime>


namespace DB
{

/** AsynchronousMetricLog is a log of metric values measured at regular time interval.
  */
struct AsynchronousMetricLogElement
{
    UInt16 event_date;
    time_t event_time;
    AsynchronousMetricValues values;

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
};

}
