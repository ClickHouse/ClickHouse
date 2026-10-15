#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>

namespace DB::QueryPlanOptimizations
{

GroupCountEstimate estimateGroupCount(const std::vector<UInt64> & key_distinct_values, std::optional<UInt64> rows, std::optional<UInt64> max_rows)
{
    if (key_distinct_values.empty())
        return GroupCountEstimate{.estimated_rows = 1, .max_rows = 1};

    GroupCountEstimate result;
    const UInt64 largest = *std::max_element(key_distinct_values.begin(), key_distinct_values.end());
    if (largest != 0)
        result.estimated_rows = rows ? std::min(largest, *rows) : largest;
    result.max_rows = max_rows;
    return result;
}

KeyDistinctValues keyDistinctValues(const RelationStats & relation, const String & column)
{
    KeyDistinctValues result;
    if (auto it = relation.column_stats.find(column); it != relation.column_stats.end())
        result.from_statistics = it->second.num_distinct_values;
    result.rows = relation.estimated_rows ? *relation.estimated_rows : relation.max_rows.value_or(0);
    return result;
}

UInt64 keyDomain(const RelationStats & relation, const String & column)
{
    if (auto it = relation.column_stats.find(column); it != relation.column_stats.end())
        return it->second.domain_distinct_values;
    return 0;
}

Float64 unionWidth(Float64 width, Float64 rows, Float64 other_width, Float64 other_rows)
{
    if (width <= 0)
        return other_width;
    if (other_width <= 0)
        return width;
    if (rows > 0 && other_rows > 0)
        return (width * rows + other_width * other_rows) / (rows + other_rows);
    return std::max(width, other_width);
}

void addUnionColumnStats(
    std::unordered_map<String, ColumnStats> & result,
    const Names & result_columns,
    const std::unordered_map<String, ColumnStats> & other,
    const Names & other_columns,
    UInt64 result_rows,
    UInt64 other_rows)
{
    /// A value of one input can be in another.
    for (auto & [_, stats] : result)
        stats.unique = false;
    for (size_t position = 0; position < result_columns.size() && position < other_columns.size(); ++position)
    {
        auto result_column = result.find(result_columns[position]);
        if (result_column == result.end())
            continue;
        auto other_column = other.find(other_columns[position]);
        const UInt64 other_distinct_values = other_column == other.end() ? 0 : other_column->second.num_distinct_values;
        const UInt64 other_max_distinct_values = other_column == other.end() ? 0 : other_column->second.max_distinct_values;
        auto & stats = result_column->second;
        stats.max_distinct_values = stats.max_distinct_values == 0 || other_max_distinct_values == 0
            ? 0
            : saturatingAdd(stats.max_distinct_values, other_max_distinct_values);
        /// The values of the union are at most the values of the inputs together; unknown when an input's are.
        stats.num_distinct_values = stats.num_distinct_values == 0 || other_distinct_values == 0
            ? 0
            : saturatingAdd(stats.num_distinct_values, other_distinct_values);
        if (other_column != other.end())
        {
            stats.domain_distinct_values = std::max(stats.domain_distinct_values, other_column->second.domain_distinct_values);
            stats.avg_bytes = unionWidth(stats.avg_bytes, Float64(result_rows), other_column->second.avg_bytes, Float64(other_rows));
        }
    }
}

namespace
{

/// Values a set of literals holds. A set from a subquery or a `Set` table is not counted: it can have
/// another type than the left side (several values then match one element) or grow during the query.
std::optional<UInt64> literalSetSize(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return {};
    const IColumn * column = node.column.get();
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
        column = &column_const->getDataColumn();
    const auto * column_set = typeid_cast<const ColumnSet *>(column);
    if (!column_set)
        return {};
    const auto future_set = column_set->getData();
    if (!future_set || future_set->isMutableDuringQuery() || !typeid_cast<const FutureSetFromTuple *>(future_set.get()))
        return {};
    const auto set = future_set->get();
    if (!set || !set->isCreated())
        return {};
    return set->getTotalRowCount();
}

/// Whether `x = c` keeps one value of `x`: a comparison that converts `x` to a wider or inexact type
/// keeps every value that converts to `c` (two `Decimal` values can compare equal to one `Float64`), and
/// a float equals both zeros.
bool equalityKeepsOneValue(const ActionsDAG::Node & column, const ActionsDAG::Node & constant)
{
    const auto column_type = removeLowCardinalityAndNullable(column.result_type);
    const auto constant_type = removeLowCardinalityAndNullable(constant.result_type);
    if (isFloat(column_type))
        return false;
    if (column_type->equals(*constant_type))
        return true;
    return isInteger(column_type) && isInteger(constant_type);
}

std::optional<Int64> constantInteger(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
        return {};
    /// A constant of the plan is a `ColumnConst`, often of zero rows.
    const auto * column_const = typeid_cast<const ColumnConst *>(node->column.get());
    if (!column_const)
        return {};
    const Field value = column_const->getField();
    if (value.getType() == Field::Types::UInt64 && value.safeGet<UInt64>() <= UInt64(std::numeric_limits<Int64>::max()))
        return Int64(value.safeGet<UInt64>());
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>();
    return {};
}

/// The length `n` of a call `substring(y, 1, n)` on a `String`, with constant arguments.
std::optional<Int64> prefixLength(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base || node->function_base->getName() != "substring"
        || node->children.size() != 3 || !isString(removeLowCardinalityAndNullable(node->result_type)))
        return {};
    const auto offset = constantInteger(node->children[1]);
    const auto length = constantInteger(node->children[2]);
    if (!offset || *offset != 1 || !length || *length <= 0)
        return {};
    return length;
}

/// `substring(y, 1, n)` is `y` itself when `y` is `substring(x, 1, m)` with `m <= n`: `y` has at most `m`
/// bytes, so the outer call keeps all of them. A projection alias is visible in `WHERE`, so a filter on
/// the alias of `substring(x, 1, m)` written as the same call filters such a prefix of the prefix.
const ActionsDAG::Node * skipPrefixOfShorterPrefix(const ActionsDAG::Node * node)
{
    while (true)
    {
        const auto outer_length = prefixLength(node);
        if (!outer_length)
            return node;
        const auto * inner = node->children[0];
        while (inner->type == ActionsDAG::ActionType::ALIAS)
            inner = inner->children.front();
        const auto inner_length = prefixLength(inner);
        if (!inner_length || *inner_length > *outer_length)
            return node;
        node = inner;
    }
}

void capDistinctValues(std::unordered_map<String, ColumnStats> & column_stats, const String & column, UInt64 values)
{
    auto & stats = column_stats[column];
    stats.max_distinct_values = stats.max_distinct_values == 0 ? values : std::min(stats.max_distinct_values, values);
}

/// The columns `node` is visible as after the step: itself when it is an input, which passes through,
/// and every output that is `node` under aliases.
std::vector<String> visibleNames(const ActionsDAG::Node * node, const ActionsDAG::NodeRawConstPtrs * outputs)
{
    std::vector<String> names;
    if (node->type == ActionsDAG::ActionType::INPUT)
        names.push_back(node->result_name);
    if (outputs)
        for (const auto * output : *outputs)
        {
            const auto * base = output;
            while (base->type == ActionsDAG::ActionType::ALIAS)
                base = base->children.front();
            if (base == node && std::ranges::find(names, output->result_name) == names.end())
                names.push_back(output->result_name);
        }
    return names;
}

}

void capDistinctValuesByFilter(
    const ActionsDAG::Node * predicate, std::unordered_map<String, ColumnStats> & column_stats, const ActionsDAG::NodeRawConstPtrs * outputs)
{
    if (!predicate)
        return;
    while (predicate->type == ActionsDAG::ActionType::ALIAS)
        predicate = predicate->children.front();
    if (predicate->type != ActionsDAG::ActionType::FUNCTION || !predicate->function_base)
        return;

    const String & name = predicate->function_base->getName();
    if (name == "and")
    {
        for (const auto * child : predicate->children)
            capDistinctValuesByFilter(child, column_stats, outputs);
        return;
    }
    if (predicate->children.size() != 2)
        return;

    const auto * lhs = predicate->children[0];
    const auto * rhs = predicate->children[1];
    const auto is_constant = [](const ActionsDAG::Node * node) { return node->type == ActionsDAG::ActionType::COLUMN && node->column; };
    std::optional<UInt64> values;
    if (name == "equals")
    {
        if (is_constant(lhs) && !is_constant(rhs))
            std::swap(lhs, rhs);
        if (is_constant(rhs) && !is_constant(lhs) && equalityKeepsOneValue(*lhs, *rhs))
            values = 1;
    }
    else if (name == "in")
    {
        values = literalSetSize(*rhs);
    }
    if (!values)
        return;
    while (lhs->type == ActionsDAG::ActionType::ALIAS)
        lhs = lhs->children.front();
    lhs = skipPrefixOfShorterPrefix(lhs);
    for (const auto & column : visibleNames(lhs, outputs))
        capDistinctValues(column_stats, column, std::max<UInt64>(*values, 1));
}

UInt64 distinctValuesAfterFilter(UInt64 distinct_values, UInt64 rows_before, UInt64 rows_after)
{
    if (distinct_values == 0 || rows_after == 0)
        return 0;
    if (rows_before == 0 || rows_after >= rows_before)
        return std::min(distinct_values, rows_after);
    const double kept = static_cast<double>(rows_after) / static_cast<double>(rows_before);
    const double rows_per_value = static_cast<double>(rows_before) / static_cast<double>(distinct_values);
    /// 1 - (1 - kept)^rows_per_value, computed so that a kept fraction below the double precision
    /// still leaves the values its rows keep.
    const double surviving = static_cast<double>(distinct_values) * -std::expm1(rows_per_value * std::log1p(-kept));
    const UInt64 bound = std::min(distinct_values, rows_after);
    return std::min<UInt64>(bound, std::max<UInt64>(1, roundToRowCount(surviving)));
}

void limitColumnStats(std::unordered_map<String, ColumnStats> & column_stats, std::optional<UInt64> rows)
{
    for (auto & [_, stats] : column_stats)
    {
        if (rows && stats.num_distinct_values > *rows)
            stats.num_distinct_values = *rows;
        stats.min_value.reset();
        stats.max_value.reset();
        stats.null_fraction.reset();
    }
}

std::optional<double> keyContainment(UInt64 side_distinct_values, UInt64 other_distinct_values, UInt64 domain_distinct_values)
{
    if (side_distinct_values == 0 || other_distinct_values == 0)
        return std::nullopt;
    if (other_distinct_values < side_distinct_values)
        return static_cast<double>(other_distinct_values) / static_cast<double>(side_distinct_values);
    if (domain_distinct_values >= other_distinct_values)
        return static_cast<double>(other_distinct_values) / static_cast<double>(domain_distinct_values);
    return 1.0;
}

void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness)
{
    bool update_left = false;
    bool update_right = false;
    if (strictness == JoinStrictness::Semi)
    {
        /// Only the output side is filtered to matching keys; the other side is not in the output.
        update_left = kind == JoinKind::Left;
        update_right = kind == JoinKind::Right;
    }
    else if (strictness != JoinStrictness::Anti)
    {
        /// An outer join preserves the named side, so only the non-preserved side can lose key values.
        update_left = kind == JoinKind::Inner || kind == JoinKind::Right
            || kind == JoinKind::Cross || kind == JoinKind::Comma;
        update_right = kind == JoinKind::Inner || kind == JoinKind::Left
            || kind == JoinKind::Cross || kind == JoinKind::Comma;
    }

    /// A zero NDV is an unknown NDV: it does not narrow the other side, and a side the join filters
    /// takes the other side's NDV as its bound.
    const UInt64 left_distinct_values = left_stats.num_distinct_values;
    const UInt64 right_distinct_values = right_stats.num_distinct_values;
    UInt64 minimum = std::min(left_distinct_values, right_distinct_values);
    if (left_distinct_values == 0)
        minimum = right_distinct_values;
    else if (right_distinct_values == 0)
        minimum = left_distinct_values;

    /// Two sides filtered out of one key domain share values in proportion to their shares of it;
    /// without this the smaller side would count as contained in the larger one, and a semi or
    /// anti join above would see every preserved key matched.
    const UInt64 domain = std::max(left_stats.domain_distinct_values, right_stats.domain_distinct_values);
    if (left_distinct_values && right_distinct_values && domain >= std::max(left_distinct_values, right_distinct_values))
    {
        const double overlap = static_cast<double>(left_distinct_values) * static_cast<double>(right_distinct_values) / static_cast<double>(domain);
        minimum = std::min(minimum, std::max<UInt64>(1, roundToRowCount(overlap)));
    }

    if (update_left)
        left_stats.num_distinct_values = minimum;
    if (update_right)
        right_stats.num_distinct_values = minimum;
    /// The key values of both sides come from the same domain.
    if (domain)
    {
        left_stats.domain_distinct_values = domain;
        right_stats.domain_distinct_values = domain;
    }
}

void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    /// Column statistics are usually absent; do not pay for a full lineage walk of the
    /// `ActionsDAG` when there is nothing to remap.
    if (mapped.empty())
        return;

    std::unordered_map<String, ColumnStats> original;
    original.swap(mapped);

    const auto lineage = traceActionsDAGLineage(actions);
    const auto & inputs = actions.getInputs();
    const auto & outputs = actions.getOutputs();
    for (const auto & output_lineage : lineage)
    {
        if (!output_lineage.input)
            continue;

        const auto stats_it = original.find(inputs[output_lineage.input->input_position]->result_name);
        if (stats_it == original.end())
            continue;

        ColumnStats stats = stats_it->second;
        /// Add the offset, guarding against overflow when the source NDV is near the maximum.
        /// An unknown NDV (zero) stays unknown; a delta on it would invent a count.
        if (stats.num_distinct_values != 0
            && stats.num_distinct_values <= std::numeric_limits<UInt64>::max() - output_lineage.input->ndv_delta)
            stats.num_distinct_values += output_lineage.input->ndv_delta;
        if (stats.max_distinct_values != 0)
            stats.max_distinct_values = saturatingAdd(stats.max_distinct_values, output_lineage.input->ndv_delta);
        /// A hop that changes the type (e.g. `toString(k)`) changes the value bytes, so drop the
        /// width to unknown.
        if (!output_lineage.input->preserves_width)
            stats.avg_bytes = 0;
        /// The value range and NULL set survive only lineage known to pass the value through
        /// unchanged; unlike NDV, they do not survive a generic deterministic function (e.g. `negate(k)`)
        /// or a `CAST`, which may rewrite NULL rows into real values.
        if (output_lineage.input->kind == ActionsDAGLineageKind::DistinctValuesBound)
        {
            stats.min_value.reset();
            stats.max_value.reset();
            stats.null_fraction.reset();
        }
        /// Such a function can map two values to one.
        if (output_lineage.input->kind == ActionsDAGLineageKind::DistinctValuesBound || output_lineage.input->ndv_delta != 0)
            stats.unique = false;
        mapped[outputs[output_lineage.output_position]->result_name] = stats;
    }

}

UInt64 toUInt64Saturating(Float64 estimate)
{
    if (!(estimate > 0))
        return 0;
    if (estimate >= static_cast<Float64>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    return static_cast<UInt64>(estimate);
}

}
