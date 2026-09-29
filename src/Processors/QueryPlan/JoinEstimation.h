#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

class WriteBuffer;

struct JoinEstimation
{
    std::optional<UInt64> output_rows;
    std::optional<UInt64> left_rows;
    std::optional<UInt64> right_rows;
    std::optional<double> cost;
    std::optional<double> selectivity;
};

/// Formatting of the estimation values for EXPLAIN PLAN, aligned with the rendering of the
/// EXPLAIN ANALYZE report metrics
String rowsEstimateToString(const std::optional<UInt64> & value);
String costEstimateToString(const std::optional<double> & value);
String selectivityEstimateToString(const std::optional<double> & value);

void describeJoinEstimation(const JoinEstimation & estimation, WriteBuffer & out, const String & prefix);

}
