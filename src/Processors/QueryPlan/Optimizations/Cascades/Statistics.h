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
class IDataType;

struct ExpressionStatistics
{
    /// Number of row that we estimated using probabilities, histograms, heuristics, etc.
    Float64 estimated_row_count = 0;
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
    virtual std::optional<Float64> getColumnAvgBytes(const String & /*table_name*/, const String & /*column_name*/) const { return std::nullopt; }
};

using OptimizerStatisticsPtr = std::unique_ptr<IOptimizerStatistics>;

OptimizerStatisticsPtr createEmptyStatistics();

class ReadFromMergeTree;

/// Bytes per row of a read: storage column sizes when available, the output header otherwise.
Float64 estimateReadBytesPerRowFromStep(const ReadFromMergeTree & read_step);

/// Average bytes per value of each column a read produces: real per-column storage sizes when the
/// parts carry them; otherwise type-based estimates, scaled to match the table-level uncompressed
/// total when only that is known (compact parts).
std::unordered_map<String, Float64> estimateReadColumnWidths(const ReadFromMergeTree & read_step);

OptimizerStatisticsPtr createStatisticsFromHint(const String & statistics_hint_json);

/// Estimate average bytes of one value of the type.
Float64 estimateColumnWidthFromType(const IDataType & type);

/// Estimate average bytes per row from a step's output header using data type information.
Float64 estimateRowWidthFromHeader(const Block & header);

/// Estimate average bytes per row of the header, preferring the columns' known average sizes
/// over the type-based estimate.
Float64 estimateRowWidth(const Block & header, const std::unordered_map<String, ColumnStats> & column_statistics);

std::optional<ExpressionStatistics> estimateStatistics(QueryPlan::Node & node);


}
