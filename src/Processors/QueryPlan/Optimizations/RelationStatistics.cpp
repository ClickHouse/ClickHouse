#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <map>
#include <string_view>

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
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsMultiColumnDistinctModel query_plan_group_multi_key_model;
    extern const SettingsMultiColumnDistinctModel query_plan_join_multi_key_model;
}

namespace DB::QueryPlanOptimizations
{

MultiColumnDistinctModel getMultiColumnDistinctModel(MultiColumnDistinctModelConsumer consumer)
{
    const auto context = CurrentThread::tryGetQueryContext();
    if (!context)
        return MultiColumnDistinctModel::Default;
    const auto & settings = context->getSettingsRef();
    return consumer == MultiColumnDistinctModelConsumer::Join ? settings[Setting::query_plan_join_multi_key_model]
                                                              : settings[Setting::query_plan_group_multi_key_model];
}

double estimateDistinctCombinations(std::vector<double> distinct_values, double rows, MultiColumnDistinctModel model)
{
    if (distinct_values.empty())
        return 1.0;
    std::sort(distinct_values.begin(), distinct_values.end(), std::greater<>());
    const double largest = distinct_values.front();
    double product = 1.0;
    for (double values : distinct_values)
        product = std::min(product * values, rows);
    double combinations = largest;
    switch (model)
    {
        case MultiColumnDistinctModel::Default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The default multi-column distinct model is not resolved");
        case MultiColumnDistinctModel::Largest:
        case MultiColumnDistinctModel::PerTable:
            break;
        case MultiColumnDistinctModel::GeometricMean:
            combinations = std::sqrt(std::min(largest, rows) * product);
            break;
        case MultiColumnDistinctModel::Backoff:
        {
            double exponent = 1.0;
            combinations = 1.0;
            for (double values : distinct_values)
            {
                combinations = std::min(combinations * std::pow(values, exponent), rows);
                exponent /= 2;
            }
            break;
        }
        case MultiColumnDistinctModel::Product:
        case MultiColumnDistinctModel::Postgres:
            combinations = product;
            break;
    }
    return std::min(combinations, rows);
}

GroupCountEstimate estimateGroupCount(const std::vector<UInt64> & key_distinct_values, std::optional<UInt64> rows, std::optional<UInt64> max_rows)
{
    std::vector<GroupKey> keys;
    for (UInt64 distinct_values : key_distinct_values)
        keys.push_back({.name = {}, .distinct_values = distinct_values, .table_rows = 0, .table_selected_rows = 0});
    return estimateGroupCount(keys, rows, max_rows);
}

namespace
{

std::string_view tableQualifier(std::string_view name)
{
    const auto dot = name.find('.');
    return dot == std::string_view::npos ? name : name.substr(0, dot);
}

/// The combinations of the keys of one table, as `estimate_num_groups` of PostgreSQL finds them.
double postgresTableCombinations(const std::vector<const GroupKey *> & keys)
{
    double largest = 0;
    double product = 1;
    UInt64 table_rows = 0;
    UInt64 selected_rows = 0;
    for (const auto * key : keys)
    {
        largest = std::max(largest, static_cast<double>(key->distinct_values));
        product *= static_cast<double>(key->distinct_values);
        table_rows = std::max(table_rows, key->table_rows);
        selected_rows = std::max(selected_rows, key->table_selected_rows);
    }
    if (keys.size() < 2 || table_rows == 0)
        return largest;
    /// The columns of a table are probably correlated, by an unknown amount.
    const double cap = std::min(static_cast<double>(table_rows), std::max(largest, 0.1 * static_cast<double>(table_rows)));
    const UInt64 cap_after_filter = distinctValuesAfterFilter(roundToRowCount(cap), table_rows, selected_rows ? selected_rows : table_rows);
    return std::min(product, std::max(largest, static_cast<double>(cap_after_filter)));
}

/// The largest NDV per table or the PostgreSQL combinations per table, multiplied over the tables.
double perTableCombinations(const std::vector<GroupKey> & keys, MultiColumnDistinctModel model, double rows)
{
    std::map<std::string_view, std::vector<const GroupKey *>> keys_per_table;
    for (const auto & key : keys)
        keys_per_table[tableQualifier(key.name)].push_back(&key);
    std::vector<double> distinct_values;
    for (const auto & [_, table_keys] : keys_per_table)
    {
        if (model == MultiColumnDistinctModel::Postgres)
            distinct_values.push_back(postgresTableCombinations(table_keys));
        else
            distinct_values.push_back(static_cast<double>(
                (*std::ranges::max_element(table_keys, {}, [](const GroupKey * key) { return key->distinct_values; }))->distinct_values));
    }
    return estimateDistinctCombinations(distinct_values, rows, MultiColumnDistinctModel::Product);
}

}

GroupCountEstimate estimateGroupCount(const std::vector<GroupKey> & keys, std::optional<UInt64> rows, std::optional<UInt64> max_rows)
{
    if (keys.empty())
        return GroupCountEstimate{.estimated_rows = 1, .max_rows = 1};

    GroupCountEstimate result;
    result.max_rows = max_rows;
    const UInt64 largest = std::ranges::max_element(keys, {}, &GroupKey::distinct_values)->distinct_values;
    if (largest != 0)
        result.estimated_rows = rows ? std::min(largest, *rows) : largest;

    /// The other models need the NDV of every key and the rows.
    auto model = getMultiColumnDistinctModel(MultiColumnDistinctModelConsumer::Group);
    if (model == MultiColumnDistinctModel::Default)
        model = MultiColumnDistinctModel::Largest;
    const bool all_keys_known = std::ranges::none_of(keys, [](const GroupKey & key) { return key.distinct_values == 0; });
    if (model == MultiColumnDistinctModel::Largest || keys.size() < 2 || !all_keys_known || !rows)
        return result;

    const double input_rows = static_cast<double>(*rows);
    double combinations = 0;
    if (model == MultiColumnDistinctModel::PerTable || model == MultiColumnDistinctModel::Postgres)
        combinations = perTableCombinations(keys, model, input_rows);
    else
    {
        std::vector<double> distinct_values;
        for (const auto & key : keys)
            distinct_values.push_back(static_cast<double>(key.distinct_values));
        combinations = estimateDistinctCombinations(distinct_values, input_rows, model);
    }
    result.estimated_rows = static_cast<UInt64>(std::llround(combinations));
    return result;
}

KeyDistinctValues keyDistinctValues(const RelationStats & relation, const String & column)
{
    KeyDistinctValues result;
    if (auto it = relation.column_stats.find(column); it != relation.column_stats.end())
    {
        result.from_statistics = it->second.num_distinct_values;
        result.imprecise = it->second.hasImpreciseDistinctValues();
    }
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

namespace
{

/// The value range and NULL fraction of a union cover the values of both inputs; unknown when an input's are.
void mergeUnionValueStats(ColumnStats & stats, const ColumnStats * other, UInt64 rows, UInt64 other_rows)
{
    const bool ranges_comparable = other && stats.min_value && stats.max_value && other->min_value && other->max_value
        && stats.value_type && other->value_type && stats.value_type->equals(*other->value_type)
        && stats.min_value->getType() == other->min_value->getType() && stats.max_value->getType() == other->max_value->getType();
    if (ranges_comparable)
    {
        stats.min_value = std::min(*stats.min_value, *other->min_value);
        stats.max_value = std::max(*stats.max_value, *other->max_value);
    }
    else
        stats.clearValueRange();

    const Float64 all_rows = Float64(rows) + Float64(other_rows);
    if (other && stats.null_fraction && other->null_fraction && all_rows > 0)
        stats.null_fraction = (*stats.null_fraction * Float64(rows) + *other->null_fraction * Float64(other_rows)) / all_rows;
    else
        stats.null_fraction.reset();
}

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
        if (other_column != other.end())
            result_column->second.rows_without_statistics
                = saturatingAdd(result_column->second.rows_without_statistics, other_column->second.rows_without_statistics);
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
        mergeUnionValueStats(stats, other_column == other.end() ? nullptr : &other_column->second, result_rows, other_rows);
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

static const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

Float64 estimatePredicateSelectivity(
    const ActionsDAG::Node * predicate,
    const std::unordered_map<String, ColumnStats> & column_stats,
    const std::function<bool(const String &, const String &)> & are_equal,
    bool & used_default)
{
    const auto * node = skipAliases(predicate);
    /// A constant filter column: the planner removes an always-false filter, so assume true.
    if (node->type == ActionsDAG::ActionType::COLUMN)
        return 1.0;
    used_default = true;
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return DEFAULT_UNKNOWN_SELECTIVITY;

    const String & name = node->function_base->getName();

    /// A default in any argument makes the whole predicate a guess; each child reports on its own.
    const auto child_selectivity = [&](const ActionsDAG::Node * child)
    {
        bool child_used_default = false;
        const Float64 selectivity = estimatePredicateSelectivity(child, column_stats, are_equal, child_used_default);
        used_default |= child_used_default;
        return selectivity;
    };
    if (name == "and")
    {
        used_default = false;
        Float64 selectivity = 1.0;
        for (const auto * child : node->children)
            selectivity *= child_selectivity(child);
        return selectivity;
    }
    if (name == "or")
    {
        used_default = false;
        Float64 none_passes = 1.0;
        for (const auto * child : node->children)
            none_passes *= 1.0 - child_selectivity(child);
        return 1.0 - none_passes;
    }
    if (name == "not" && node->children.size() == 1)
    {
        used_default = false;
        return 1.0 - estimatePredicateSelectivity(node->children.front(), column_stats, are_equal, used_default);
    }

    /// A runtime join filter repeats the join selectivity, which the join estimate carries.
    if (name == "__applyFilter")
    {
        used_default = false;
        return 1.0;
    }
    if (name == "like" || name == "ilike")
        return DEFAULT_LIKE_SELECTIVITY;
    if (name == "notLike" || name == "notILike")
        return 1.0 - DEFAULT_LIKE_SELECTIVITY;
    if (name == "isNull")
        return DEFAULT_EQUALITY_SELECTIVITY;
    if (name == "isNotNull")
        return 1.0 - DEFAULT_EQUALITY_SELECTIVITY;

    const bool is_equals = name == "equals";
    const bool is_not_equals = name == "notEquals";
    const bool is_range = name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals";
    if ((is_equals || is_not_equals || is_range) && node->children.size() == 2)
    {
        if (is_range)
            return DEFAULT_RANGE_SELECTIVITY;

        const auto * left = skipAliases(node->children[0]);
        const auto * right = skipAliases(node->children[1]);
        const bool left_is_constant = left->column && isColumnConst(*left->column);
        const bool right_is_constant = right->column && isColumnConst(*right->column);

        /// An NDV that rows without statistics could double is as good as unknown.
        auto column_ndv = [&](const ActionsDAG::Node * side) -> UInt64
        {
            auto it = column_stats.find(side->result_name);
            if (it == column_stats.end() || it->second.hasImpreciseDistinctValues())
                return 0;
            return it->second.num_distinct_values;
        };
        auto has_imprecise_ndv = [&](const ActionsDAG::Node * side)
        {
            auto it = column_stats.find(side->result_name);
            return it != column_stats.end() && it->second.hasImpreciseDistinctValues();
        };

        Float64 equal_selectivity = DEFAULT_EQUALITY_SELECTIVITY;
        if (!left_is_constant && !right_is_constant)
        {
            /// Two columns. An equality the plan below already enforces (e.g. the keys of an
            /// inner join under this filter) holds on every row; otherwise the join-equality
            /// formula 1 / max(NDV) applies.
            if (are_equal(left->result_name, right->result_name))
            {
                equal_selectivity = 1.0;
                used_default = false;
            }
            else if (UInt64 max_ndv = std::max(column_ndv(left), column_ndv(right));
                     max_ndv && !has_imprecise_ndv(left) && !has_imprecise_ndv(right))
            {
                equal_selectivity = 1.0 / Float64(max_ndv);
                used_default = false;
            }
        }
        else
        {
            const auto * column_side = left_is_constant ? right : left;
            if (UInt64 ndv = column_ndv(column_side))
            {
                equal_selectivity = 1.0 / Float64(ndv);
                used_default = false;
            }
        }
        return is_equals ? equal_selectivity : 1.0 - equal_selectivity;
    }

    return DEFAULT_UNKNOWN_SELECTIVITY;
}

void limitColumnStats(std::unordered_map<String, ColumnStats> & column_stats, std::optional<UInt64> rows)
{
    for (auto & [_, stats] : column_stats)
    {
        if (rows && stats.num_distinct_values > *rows)
            stats.num_distinct_values = *rows;
        stats.clearValueRange();
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

    /// The narrowed keys take values from both sides, so the rows without statistics of each side can add values.
    const UInt64 rows_without_statistics = saturatingAdd(left_stats.rows_without_statistics, right_stats.rows_without_statistics);
    if (update_left)
    {
        left_stats.num_distinct_values = minimum;
        left_stats.rows_without_statistics = rows_without_statistics;
    }
    if (update_right)
    {
        right_stats.num_distinct_values = minimum;
        right_stats.rows_without_statistics = rows_without_statistics;
    }
    /// The key values of both sides come from the same domain.
    if (domain)
    {
        left_stats.domain_distinct_values = domain;
        right_stats.domain_distinct_values = domain;
    }
}

namespace
{

/// The range of `output` values, when `output` is a chain of one-argument functions of `input` that is
/// monotonic on the range of the input values.
std::optional<Range> mapRangeThroughMonotonicChain(const ActionsDAG::Node & output, const ActionsDAG::Node & input, const ColumnStats & stats)
{
    if (!stats.min_value || !stats.max_value || !stats.value_type)
        return {};
    /// A range of another type has another meaning for the same `Field` (a `JOIN USING` can cast the column).
    const auto input_type = removeLowCardinalityAndNullable(input.result_type);
    if (!removeLowCardinalityAndNullable(stats.value_type)->equals(*input_type))
        return {};
    /// The statistics of a floating-point column leave `NaN` out of its range, and a function can map `NaN`
    /// outside the range it maps the other values to.
    if (isFloat(input_type))
        return {};

    KeyCondition::MonotonicFunctionsChain chain;
    for (const auto * node = &output; node != &input; node = node->children.front())
    {
        if (node->type == ActionsDAG::ActionType::ALIAS)
            continue;
        if (node->type != ActionsDAG::ActionType::FUNCTION || node->children.size() != 1
            || !node->function_base->hasInformationAboutMonotonicity())
            return {};
        chain.push_back(node->function_base);
    }
    std::ranges::reverse(chain);
    return KeyCondition::applyMonotonicFunctionsChainToRange(
        Range(*stats.min_value, true, *stats.max_value, true), chain, /*first_function_number=*/ 0, input.result_type);
}

/// Number of integers in a closed range whose bounds are 64-bit integers.
std::optional<UInt64> countIntegersInRange(const Range & range)
{
    const auto left_type = range.left.getType();
    if (left_type != range.right.getType())
        return {};

    UInt64 width = 0;
    if (left_type == Field::Types::UInt64)
    {
        const auto left = range.left.safeGet<UInt64>();
        const auto right = range.right.safeGet<UInt64>();
        if (right < left)
            return {};
        width = right - left;
    }
    else if (left_type == Field::Types::Int64)
    {
        const auto left = range.left.safeGet<Int64>();
        const auto right = range.right.safeGet<Int64>();
        if (right < left)
            return {};
        width = static_cast<UInt64>(right) - static_cast<UInt64>(left);
    }
    else
        return {};
    return saturatingAdd(width, UInt64{1});
}

/// A monotonic function maps the input range into the range between its values at the ends, so an
/// integer result has at most as many values as that range holds integers (e.g. `toYear(d)` of a decade
/// of dates has at most 11). Statistics of some parts only can miss values outside the range, so the
/// bound caps the estimates but not `max_distinct_values`.
void boundStatsByMonotonicRange(ColumnStats & stats, const Range & output_range, const DataTypePtr & output_type)
{
    stats.min_value = static_cast<const Field &>(output_range.left);
    stats.max_value = static_cast<const Field &>(output_range.right);
    stats.value_type = output_type;
    const auto integers = countIntegersInRange(output_range);
    if (!integers)
        return;
    if (stats.num_distinct_values != 0)
        stats.num_distinct_values = std::min(stats.num_distinct_values, *integers);
    if (stats.domain_distinct_values != 0)
        stats.domain_distinct_values = std::min(stats.domain_distinct_values, *integers);
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
        /// unchanged; unlike NDV, they do not survive a generic deterministic function (e.g. `cityHash64(k)`)
        /// or a `CAST`, which may rewrite NULL rows into real values. A function chain that is monotonic
        /// on the value range maps the range instead, unless it turns NULL rows into values (`assumeNotNull`
        /// exposes whatever values are stored under NULL, which the range does not cover).
        if (output_lineage.input->kind == ActionsDAGLineageKind::DistinctValuesBound)
        {
            const auto & output = *outputs[output_lineage.output_position];
            std::optional<Range> output_range;
            if (output_lineage.input->ndv_delta == 0)
                output_range = mapRangeThroughMonotonicChain(output, *inputs[output_lineage.input->input_position], stats);
            stats.clearValueRange();
            stats.null_fraction.reset();
            if (output_range)
                boundStatsByMonotonicRange(stats, *output_range, output.result_type);
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
