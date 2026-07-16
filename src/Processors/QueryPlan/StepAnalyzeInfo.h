#pragma once

#include <base/types.h>
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
    UInt64 matched_left = 0;
    UInt64 right_rows = 0;
    UInt64 matched_right = 0;
};

inline MetricList joinSideMetrics(UInt64 rows, UInt64 matched)
{
    MetricList metrics;
    metrics.emplace_back("rows", rows, StepMetric::Format::Quantity);
    metrics.emplace_back("matched", matched, StepMetric::Format::Quantity);
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
