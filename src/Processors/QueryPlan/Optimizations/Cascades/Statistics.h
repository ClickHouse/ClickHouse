#pragma once

#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/Block.h>
#include <base/types.h>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB
{

class WriteBuffer;

struct ExpressionStatistics
{
    /// Number of row that we estimated using probabilities, histograms, heuristics, etc.
    Float64 estimated_row_count;
    /// Proven minimum number of rows
    Float64 min_row_count = 0;
    /// Proven maximum number of rows. E.g. after LIMIT step
    Float64 max_row_count = Float64(std::numeric_limits<UInt64>::max());

    /// Estimated average number of bytes per row in the output of this expression.
    /// Used to convert row-based costs into byte-based costs for network/memory/IO.
    Float64 estimated_bytes_per_row = 1.0;

    /// Statistics for output columns of the expression
    std::unordered_map<String, ColumnStats> column_statistics;

    void dump(WriteBuffer & out) const;
    String dump() const;
};

class IOptimizerStatistics
{
public:
    virtual ~IOptimizerStatistics() = default;
    virtual std::optional<UInt64> getCardinality(const String & table_name) const = 0;
    virtual std::optional<UInt64> getNumberOfDistinctValues(const String & table_name, const String & column_name) const = 0;
    virtual std::optional<Float64> getAvgRowBytes(const String & table_name) const = 0;
};

using OptimizerStatisticsPtr = std::unique_ptr<IOptimizerStatistics>;

OptimizerStatisticsPtr createEmptyStatistics();
OptimizerStatisticsPtr createStatisticsFromHint(const String & statistics_hint_json);

/// Estimate average bytes per row from a step's output header using data type information.
Float64 estimateRowWidthFromHeader(const Block & header);

std::optional<ExpressionStatistics> estimateStatistics(QueryPlan::Node & node);


}
