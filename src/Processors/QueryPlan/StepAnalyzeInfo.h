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

    enum class Format { Raw, Bytes, Quantity, Time, Percent };
    Format format = Format::Raw;
};

using StepAnalyzeInfo = std::vector<StepMetric>;

struct JoinMatchStats
{
    UInt64 total_left_rows = 0;
    /// Left rows with at least one match on the right side.
    UInt64 matched_left_rows = 0;
};

inline void appendJoinMatchStats(StepAnalyzeInfo & info, const JoinMatchStats & stats)
{
    info.emplace_back("matched rows", stats.matched_left_rows, StepMetric::Format::Quantity);
    const double match_rate = stats.total_left_rows
        ? 100.0 * static_cast<double>(stats.matched_left_rows) / static_cast<double>(stats.total_left_rows)
        : 0.0;
    info.emplace_back("match rate", match_rate, StepMetric::Format::Percent);
}

}
