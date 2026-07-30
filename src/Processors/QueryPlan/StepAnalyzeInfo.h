#pragma once

#include <base/types.h>
#include <optional>
#include <string>
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

    std::optional<double> share_of_stage_time;
};

using MetricList = std::vector<StepMetric>;

struct MetricGroup
{
    std::string label;
    MetricList metrics;
};

using StepAnalysisReport = std::vector<MetricGroup>;

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
