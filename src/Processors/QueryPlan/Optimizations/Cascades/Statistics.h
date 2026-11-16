#pragma once

#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/EquivalenceClasses.h>
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
    /// Number of rows estimated using probabilities, histograms, heuristics, etc.
    Float64 estimated_row_count = 0;
    /// Proven minimum number of rows
    Float64 min_row_count = 0;
    /// Proven maximum number of rows. E.g. after a `LIMIT` step
    Float64 max_row_count = Float64(std::numeric_limits<UInt64>::max());

    /// Estimated upper bound on the number of distinct rows. Tighter than `estimated_row_count`
    /// when an operator keeps duplicates but limits the possible values: every `INTERSECT` output
    /// value occurs in every input. A `Distinct` above clamps its estimate by this bound even
    /// when the distinct columns have no NDV statistics.
    Float64 estimated_distinct_bound = Float64(std::numeric_limits<UInt64>::max());

    /// Estimated average number of bytes per row in the output of this expression.
    /// Used to convert row-based costs into byte-based costs for network/memory/IO.
    Float64 estimated_bytes_per_row = 1.0;

    /// Bytes a table-read expression has to scan, as opposed to the bytes it outputs. A filter
    /// that the primary key cannot prune leaves the whole granule selection to scan, so the
    /// scan volume can exceed the output estimate by orders of magnitude. Zero when unknown or
    /// for non-read expressions; only the read cost consults it.
    Float64 physical_read_bytes = 0;

    /// Statistics for output columns of the expression
    std::unordered_map<String, ColumnStats> column_statistics;

    /// Classes of output columns that hold equal values on every row.
    EquivalenceClasses<String> equivalences;

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
    virtual std::optional<Float64> getAvgColumnBytes(const String & /*table_name*/, const String & /*column_name*/) const { return std::nullopt; }
};

using OptimizerStatisticsPtr = std::unique_ptr<IOptimizerStatistics>;

OptimizerStatisticsPtr createEmptyStatistics();

class ReadFromMergeTree;

/// Average bytes per value of each column a read produces: real per-column storage sizes when the
/// parts carry them (scaled up to the table-level uncompressed total when compact parts hide part
/// of the data); otherwise type-based estimates scaled to match the measured row width.
std::unordered_map<String, Float64> estimateReadColumnWidths(const ReadFromMergeTree & read_step);

/// Column widths for a read whose table-level row width is hinted: type-based estimates scaled so
/// their sum over the table's columns matches the hinted row width, ignoring the parts' sizes.
std::unordered_map<String, Float64> estimateReadColumnWidthsScaledToRow(const ReadFromMergeTree & read_step, Float64 row_bytes);

OptimizerStatisticsPtr createStatisticsFromHint(const String & statistics_hint_json);

/// Estimate average bytes of one value of the type.
Float64 estimateColumnWidthFromType(const IDataType & type);

/// Estimate average bytes per row from a step's output header using data type information.
Float64 estimateRowWidthFromHeader(const Block & header);

/// Estimate average bytes per row of the header, preferring the columns' known average sizes
/// over the type-based estimate.
Float64 estimateRowWidth(const Block & header, const std::unordered_map<String, ColumnStats> & column_statistics);

std::optional<ExpressionStatistics> estimateStatistics(QueryPlan::Node & node);

/// Sets `physical_read_bytes` from the rows the primary key keeps (`physical_selected_rows`,
/// from the index analysis): a filter off the sorting key prunes no granules, and each
/// surviving granule is read whole. The output estimate acts as a lower bound: stat hints mark
/// tiny stand-in tables whose physical selection says nothing about the pretended size.
void fillPhysicalReadBytes(ExpressionStatistics & statistics, Float64 physical_selected_rows);


}
