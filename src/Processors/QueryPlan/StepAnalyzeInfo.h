#pragma once

#include <base/types.h>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace DB
{

struct StepMetric
{
    std::string name;
    std::variant<Int64, UInt64, double, std::string> value;

    enum class Format { Raw, Bytes, Quantity, Time, Percent, Ratio };
    Format format = Format::Raw;
};

using MetricList = std::vector<StepMetric>;

struct MetricGroup
{
    std::string label;
    MetricList metrics;
};

using StepAnalysisReport = std::vector<MetricGroup>;

inline std::optional<UInt64> findQuantity(const MetricGroup & group, std::string_view name)
{
    for (const auto & metric : group.metrics)
    {
        if (metric.name == name)
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

inline MetricList joinSideMetrics(UInt64 rows, std::optional<UInt64> matched)
{
    MetricList metrics;
    metrics.emplace_back("rows", rows, StepMetric::Format::Quantity);
    if (matched)
        metrics.emplace_back("matched", *matched, StepMetric::Format::Quantity);
    else
        metrics.emplace_back("matched", std::string("not collected"), StepMetric::Format::Raw);
    return metrics;
}

inline StepAnalysisReport buildMatchedRowsReport(const JoinAnalysisCounters & counters)
{
    StepAnalysisReport report;
    report.push_back({"left", joinSideMetrics(counters.left_rows, counters.matched_left)});
    report.push_back({"right", joinSideMetrics(counters.right_rows, counters.matched_right)});
    return report;
}

}
