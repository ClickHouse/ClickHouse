#pragma once

#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include <Core/Joins.h>
#include <Core/Names.h>
#include <Core/SettingsEnums.h>
#include <Processors/QueryPlan/CostEstimationInfo.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <base/types.h>

namespace DB
{

class ActionsDAG;

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    /// Rows the relation cannot exceed: the selected rows of a read, carried through the steps above
    /// it by their semantics (a filter keeps the bound, a limit lowers it). Known for many relations
    /// whose point estimate is unknown, so a missing point does not have to be costed as one row.
    std::optional<UInt64> max_rows = {};
    std::optional<Float64> avg_row_bytes = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
    bool imprecise_estimate = false;
    /// A default selectivity stood in for a predicate the statistics could not estimate somewhere
    /// below, so `estimated_rows` is a guess; the steps above a read carry it.
    bool estimate_from_defaults = false;

    /// `table_name` is a chain of sub-joins that already shows their estimates, so `EXPLAIN` renders
    /// the chain on its own instead of appending one more estimate to it.
    bool composite = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;
};

/// The plan node annotation of a relation estimate, for `EXPLAIN estimates = 1` and the
/// processors profile log. A missing row count stays missing.
inline CostEstimationInfo toCostEstimationInfo(const RelationStats & stats)
{
    return CostEstimationInfo{
        .rows = stats.estimated_rows ? std::optional<Float64>(Float64(*stats.estimated_rows)) : std::nullopt,
        .cost = std::nullopt,
        .source = stats.source,
        .max_rows = stats.max_rows ? std::optional<Float64>(Float64(*stats.max_rows)) : std::nullopt,
        .estimate_from_defaults = stats.estimate_from_defaults};
}

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);

/// Distinct value combinations of a set of keys among `rows` rows: the rows of an aggregation or a
/// `DISTINCT` over them.
struct GroupCountEstimate
{
    /// Unknown when no key has an NDV.
    std::optional<UInt64> estimated_rows;
    /// The rows the input cannot exceed; the key NDVs are estimates and do not tighten it.
    std::optional<UInt64> max_rows;
};

/// Width of a `UNION ALL` row or value from the widths of two inputs (zero = unknown): weighted by
/// the inputs' rows, the wider one when the rows are unknown.
Float64 unionWidth(Float64 width, Float64 rows, Float64 other_width, Float64 other_rows);

/// Column statistics of a `UNION ALL`: adds the `other` input's values to `result`, matching the
/// columns by position (`result_columns` names the output, `other_columns` the other input); the
/// rows of both inputs (zero = unknown) weight the value widths.
void addUnionColumnStats(
    std::unordered_map<String, ColumnStats> & result,
    const Names & result_columns,
    const std::unordered_map<String, ColumnStats> & other,
    const Names & other_columns,
    UInt64 result_rows,
    UInt64 other_rows);

/// The largest known key NDV, capped by the rows: the groups are at least that many when the NDV is
/// exact, and real keys are correlated more often than not. `key_distinct_values` holds one NDV per
/// key, zero for a key without one. No keys: one group.
GroupCountEstimate estimateGroupCount(const std::vector<UInt64> & key_distinct_values, std::optional<UInt64> rows, std::optional<UInt64> max_rows);

/// A key of an aggregation or a `DISTINCT` with what the models of `MultiColumnDistinctModel` need: the NDV
/// (zero when unknown), the name, whose qualifier tells the table, and the rows of that table (see `ColumnStats`).
struct GroupKey
{
    std::string_view name;
    UInt64 distinct_values = 0;
    UInt64 table_rows = 0;
    UInt64 table_selected_rows = 0;
};

/// The same, for the model the setting `query_plan_group_multi_key_model` selects.
GroupCountEstimate estimateGroupCount(const std::vector<GroupKey> & keys, std::optional<UInt64> rows, std::optional<UInt64> max_rows);

/// How the distinct value combinations of several key columns follow from the NDVs of the columns, for the group
/// count of `GROUP BY` and `DISTINCT` and for each side of a join key of several columns; the settings
/// `query_plan_group_multi_key_model` and `query_plan_join_multi_key_model` describe the models. A table of a column is
/// the qualifier of its name. All of them are at most the rows of the input.
using DB::MultiColumnDistinctModel;

/// The estimate that uses a model: the selectivity of a join key or the group count of an aggregation or `DISTINCT`.
enum class MultiColumnDistinctModelConsumer : uint8_t
{
    Join,
    Group,
};

/// The model the setting of `consumer` selects; `Default` without a query.
MultiColumnDistinctModel getMultiColumnDistinctModel(MultiColumnDistinctModelConsumer consumer);

/// The distinct value combinations of key columns with the given NDVs (all known) under `model`, at most `rows`.
/// `Default` is not a model of its own here; resolve it for the caller first.
double estimateDistinctCombinations(std::vector<double> distinct_values, double rows, MultiColumnDistinctModel model);

/// Distinct values of a join key: the NDV its column statistics give, and the rows of its relation,
/// which stand in for a key without an NDV (the key then counts as unique); a relation without a
/// row estimate gives the rows it cannot exceed. Zero means unknown.
struct KeyDistinctValues
{
    UInt64 from_statistics = 0;
    UInt64 rows = 0;
    /// `from_statistics` could be too low, see `ColumnStats::hasImpreciseDistinctValues`.
    bool imprecise = false;

    UInt64 orRows() const { return from_statistics ? from_statistics : rows; }
};

KeyDistinctValues keyDistinctValues(const RelationStats & relation, const String & column);
/// NDV of the whole base column the key comes from; zero when unknown.
UInt64 keyDomain(const RelationStats & relation, const String & column);

/// Distinct values of a column left by a filter that keeps `rows_after` of `rows_before` rows, when
/// the column's `distinct_values` are spread evenly over the rows: a value survives when any of its
/// rows does. Zero when the NDV is unknown; never above `rows_after`.
UInt64 distinctValuesAfterFilter(UInt64 distinct_values, UInt64 rows_before, UInt64 rows_after);

/// Selectivity factors for predicates without usable statistics; the same values that
/// `ConditionSelectivityEstimator` uses for the reads.
constexpr Float64 DEFAULT_EQUALITY_SELECTIVITY = 0.01;
constexpr Float64 DEFAULT_RANGE_SELECTIVITY = 0.33;
constexpr Float64 DEFAULT_UNKNOWN_SELECTIVITY = 0.33;
constexpr Float64 DEFAULT_LIKE_SELECTIVITY = 0.1;

/// The fraction of rows that pass `predicate`, from the distinct values in `column_stats`; they carry no
/// value ranges here, so an equality uses 1/NDV and everything else the default factors. `are_equal` tells
/// whether the plan below already makes two columns equal on every row, as the keys of an inner join do.
/// `used_default` is set when a default factor stood in for a predicate the statistics could not estimate.
Float64 estimatePredicateSelectivity(
    const ActionsDAG::Node * predicate,
    const std::unordered_map<String, ColumnStats> & column_stats,
    const std::function<bool(const String &, const String &)> & are_equal,
    bool & used_default);

/// Column statistics of the rows a limit keeps, at most `rows` of them when the limit bounds the rows: a
/// column has at most as many values as rows. The kept rows are not a value-uniform sample (a TopN keeps
/// one end of the sorted range), so the value ranges and NULL fractions of the input do not describe them.
void limitColumnStats(std::unordered_map<String, ColumnStats> & column_stats, std::optional<UInt64> rows);

/// Sets `max_distinct_values` of the columns `predicate` limits to constants, for each conjunct: one
/// value for `x = c` without a lossy conversion, the size of the set for `x IN (c1, ...)` with a set of
/// literals. `x` is an input of the step, which passes through, or one of `outputs` (the step's outputs,
/// may be null). A column without an entry gets one.
void capDistinctValuesByFilter(
    const ActionsDAG::Node * predicate,
    std::unordered_map<String, ColumnStats> & column_stats,
    const ActionsDAG::NodeRawConstPtrs * outputs = nullptr);

/// Share of one side's key values that the other side also has, from the two NDVs (zero =
/// unknown) and the key's domain: fewer values on the other side all lie within this side's; as
/// many or more meet this side's in proportion to their share of the domain, all of them when the
/// domain is unknown. Nothing when either NDV is unknown.
std::optional<double> keyContainment(UInt64 side_distinct_values, UInt64 other_distinct_values, UInt64 domain_distinct_values = 0);

/// Tighten equi-join key NDVs to their minimum, respecting which side each join kind preserves.
/// Anti joins and full joins leave both inputs unchanged. A zero NDV is unknown: it is bounded by
/// the other side's NDV when the join filters its side, and never narrows the other side.
void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness);

/// Converts a row or distinct-value estimate to `UInt64`. Estimates can exceed the `UInt64` range (a cross
/// product of large inputs), so the result saturates at the maximum; NaN and non-positive values give 0.
UInt64 toUInt64Saturating(Float64 estimate);

}

}
