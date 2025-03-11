#pragma once

#include <base/Decimal.h>
#include <Storages/ColumnsDescription.h>
#include <Common/LatencyBuckets.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/PeriodicLog.h>

namespace DB
{

struct LatencyLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    std::vector<std::vector<LatencyBuckets::Count>> latency_buckets_values;

    static std::string name() { return "LatencyLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class LatencyLog : public PeriodicLog<LatencyLogElement>
{
    using PeriodicLog<LatencyLogElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override;
};

}
