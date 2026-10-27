#pragma once

#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Common/EquivalenceClasses.h>
#include <Core/Block.h>
#include <base/types.h>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

namespace DB
{

class WriteBuffer;
class IDataType;

struct ExpressionStatistics
{
    /// Number of rows estimated using probabilities, histograms, heuristics, etc.
    Float64 estimated_row_count = 0;
    /// No estimate exists for this result: `estimated_row_count` is a search value for the cost
    /// model (the proven upper bound when there is one, else the largest known leaf of the query),
    /// never reported as an estimate. Set for a read the estimator could not estimate and for
    /// everything derived from it.
    bool rows_unknown = false;
    /// A default selectivity stood in for a predicate somewhere below: `estimated_row_count` is a
    /// guess, which the replication budget does not take on its own.
    bool estimate_from_defaults = false;
    /// The estimate has another imprecise part, see `RelationStats::imprecise_estimate`: statistics that cover only
    /// some parts of a table, a key without statistics, the rows of a past execution.
    bool imprecise_estimate = false;
    /// The search value of this unknown result is the product of an unknown join's inputs and a
    /// stand-in key selectivity, which can be far below the rows the join produces; the cost model
    /// does not price a gather of such a result on it. Kept by the operators that copy their
    /// input's statistics, dropped by the ones that derive their own.
    bool search_value_is_join_product = false;
    /// Where `estimated_row_count` comes from, for `EXPLAIN estimates = 1` and the profile log. An
    /// operator that copies its input's statistics keeps it; a join or a set operation has none.
    RowEstimateSource source = RowEstimateSource::NoSource;
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

    /// The rows as the shared estimator sees them: none when unknown.
    std::optional<UInt64> knownRows() const;
    /// The bound as the shared estimator sees it: none when unbounded.
    std::optional<UInt64> knownBound() const;
    /// The statistics as a relation for the shared estimator.
    RelationStats toRelationStats() const;
    /// Takes the rows, the bound, the column statistics and the flags of a relation estimate; a
    /// missing estimate leaves the bound as the search value.
    void applyRelationStats(const RelationStats & relation_stats);

    /// Takes the imprecision of an input this result is derived from.
    void addImprecisionOf(const ExpressionStatistics & input)
    {
        estimate_from_defaults |= input.estimate_from_defaults;
        imprecise_estimate |= input.imprecise_estimate;
    }

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
