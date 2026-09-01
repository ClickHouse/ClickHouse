#pragma once

#include <base/types.h>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace DB
{

enum class MetricGroupKey : UInt8
{
    IO,
    Left,
    Right,
    HashTable,
    Buffer,
    Spill,
    Build,
    Probe,
};

enum class MetricKey : UInt8
{
    Unnamed,

    InputRows,
    OutputRows,
    InputBytes,
    OutputBytes,

    Rows,
    Matched,
    MatchRate,
    Fanout,

    UniqueKeys,
    Memory,
    Buckets,
    Rehashes,

    LeftSpilled,
    RightSpilled,
    Spilled,
    Compressed,

    Size,
    Blocks,
    Storage,

    SortTime,
    SortShare,

    Min,
    Median,
    Max,
    Sum,
};

enum class MetricFormat : UInt8
{
    Raw,
    Bytes,
    Quantity,
    Time,
    Percent,
    Ratio,
};

std::string_view toString(MetricGroupKey key);
std::string_view toString(MetricKey key);

MetricFormat formatOf(MetricKey key);

using MetricValue = std::variant<std::monostate, Int64, UInt64, double, std::string>;

struct StepMetric
{
    MetricKey key = MetricKey::Unnamed;
    MetricValue value;
};

using MetricList = std::vector<StepMetric>;

struct MetricGroup
{
    MetricGroupKey key = MetricGroupKey::IO;
    MetricList metrics;
};

using StepAnalysisReport = std::vector<MetricGroup>;

inline const MetricGroup * findGroup(const StepAnalysisReport & report, MetricGroupKey key)
{
    for (const auto & group : report)
        if (group.key == key)
            return &group;
    return nullptr;
}

inline MetricGroup * findGroup(StepAnalysisReport & report, MetricGroupKey key)
{
    for (auto & group : report)
        if (group.key == key)
            return &group;
    return nullptr;
}

inline std::optional<UInt64> findQuantity(const MetricGroup & group, MetricKey key)
{
    for (const auto & metric : group.metrics)
    {
        if (metric.key == key)
        {
            if (const auto * quantity = std::get_if<UInt64>(&metric.value))
                return *quantity;
            break;
        }
    }
    return std::nullopt;
}

struct JoinAnalysisCounters
{
    UInt64 left_rows = 0;
    std::optional<UInt64> matched_left;
    UInt64 right_rows = 0;
    std::optional<UInt64> matched_right;
};

/// Sums a per-side matched counter over the parts a join is split into. A part that cannot
/// report the metric makes the whole sum unavailable, since summing the rest would understate it.
class MatchedRowsAccumulator
{
public:
    void add(std::optional<UInt64> value)
    {
        if (available && value)
            total += *value;
        else
            available = false;
    }

    std::optional<UInt64> get() const { return available ? std::optional<UInt64>(total) : std::nullopt; }

private:
    UInt64 total = 0;
    bool available = true;
};

inline MetricValue optionalQuantity(std::optional<UInt64> value)
{
    if (value)
        return *value;
    return std::monostate{};
}

inline MetricList joinSideMetrics(UInt64 rows, std::optional<UInt64> matched)
{
    MetricList metrics;
    metrics.emplace_back(MetricKey::Rows, rows);
    metrics.emplace_back(MetricKey::Matched, optionalQuantity(matched));
    return metrics;
}

inline StepAnalysisReport buildMatchedRowsReport(const JoinAnalysisCounters & counters)
{
    StepAnalysisReport report;
    report.push_back({MetricGroupKey::Left, joinSideMetrics(counters.left_rows, counters.matched_left)});
    report.push_back({MetricGroupKey::Right, joinSideMetrics(counters.right_rows, counters.matched_right)});
    return report;
}

}
