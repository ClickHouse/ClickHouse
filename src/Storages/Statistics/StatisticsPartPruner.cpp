#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <map>
#include <unordered_set>

namespace DB
{

namespace
{

/// Create a Range from statistics estimate for use in part pruning.
/// MinMax statistics now store typed Field values, so we can directly construct Range
/// without lossy Float64 conversions.
///
/// NULL handling: a Nullable column's NULL values sort at POSITIVE_INFINITY. When the NULL
/// count is known (`Basic` statistics on a nullable column), the range can be tightened:
///   - null_count == 0: no NULLs in the part, the right bound is the real max;
///   - null_count == rows_count: the part is all-NULL, represented by the [+inf, +inf] sentinel
///     range (it intersects nothing except ranges that reach the NULL sentinel);
///   - otherwise the right bound stays POSITIVE_INFINITY to cover possible NULLs.
///
/// Returns std::nullopt when statistics are unavailable or corrupted,
/// causing the caller to fall back to a whole-universe Range (no pruning).
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, bool is_nullable)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    const std::optional<UInt64> & null_count = estimate.estimated_null_count;
    if (null_count.has_value() && *null_count > estimate.rows_count)
        return std::nullopt;

    if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
        return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);

    const bool no_nulls = !is_nullable || (null_count.has_value() && *null_count == 0);

    if (estimate.estimated_min.has_value() && estimate.estimated_max.has_value())
    {
        const Field & min_value = estimate.estimated_min.value();
        const Field & max_value = estimate.estimated_max.value();

        /// min > max is a legacy sentinel pair or corrupted statistics.
        if (min_value > max_value)
            return std::nullopt;

        if (no_nulls)
            return Range(min_value, true, max_value, true);

        /// Nullable column that may contain NULLs: keep the right bound at the NULL sentinel.
        return Range(min_value, true, POSITIVE_INFINITY, true);
    }

    /// No min/max (non-numeric type like String/Array/Tuple/Map): a known-zero NULL count
    /// still excludes the NULL sentinel.
    if (is_nullable && no_nulls)
        return Range::createWholeUniverseWithoutNull();

    return std::nullopt;
}

/// Returns true when a column's statistics description can produce a numeric min/max range for
/// part pruning: an explicit `MinMax` statistic, or `Basic` on a numeric/temporal column.
/// Nullable non-numeric `Basic` statistics (NULL count only) are not sufficient on their own:
/// `LIKE '%x%'` on `Nullable(String)` cannot use that count, and treating it as prunable would
/// load per-part statistics with no chance to prune.
bool statisticsHasMinMaxForPartPruning(const ColumnStatisticsDescription & stats_desc)
{
    if (stats_desc.types_to_desc.contains(StatisticsType::MinMax))
        return true;
    if (stats_desc.types_to_desc.contains(StatisticsType::Basic))
        return removeLowCardinalityAndNullable(stats_desc.data_type)->isValueRepresentedByNumber();
    return false;
}

/// If `name` is the virtual `<parent>.null` null-map (from `IS NULL` rewritten by
/// `optimize_functions_to_subcolumns`), return the parent column. A physical column
/// literally named `foo.null` wins. A parent whose nested type owns a `null` subcolumn
/// (e.g. `Nullable(JSON)`, where `json.null` is a `Dynamic` path) is excluded.
std::optional<String> tryResolveNullMapParent(const ColumnsDescription & columns, const String & name)
{
    static constexpr std::string_view suffix = ".null";
    if (!name.ends_with(suffix) || columns.tryGet(name))
        return std::nullopt;

    String parent = name.substr(0, name.size() - suffix.size());
    const auto * parent_col = columns.tryGet(parent);
    if (!parent_col || removeLowCardinalityAndNullable(parent_col->type)->hasSubcolumn("null"))
        return std::nullopt;
    return parent;
}

bool hasBasicStatsOnNullableType(const ColumnDescription & col)
{
    return col.statistics.types_to_desc.contains(StatisticsType::Basic)
        && isNullableOrLowCardinalityNullable(col.type);
}

bool rejectsNullCountWitness(const KeyCondition & key_condition, const DataTypePtr & type)
{
    const DataTypes types{type};
    const bool all_null_possible = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true)), types).can_be_true;
    const bool no_null_possible = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range::createWholeUniverseWithoutNull()), types).can_be_true;
    return !all_null_possible || !no_null_possible;
}

bool isConstantZero(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return false;

    const Field value = (*node.column)[0];
    if (value.getType() == Field::Types::UInt64)
        return value.safeGet<UInt64>() == 0;
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>() == 0;
    return false;
}

/// Collect top-level `AND` conjuncts testing a column's NULL-ness: a bare `<col>.null`
/// input, `not(<col>.null)`, `<col>.null != 0` / `<col>.null = 0` (the
/// `optimize_functions_to_subcolumns` rewrite of `isNull` / the complementary form),
/// or `isNull(<col>)` / `isNotNull(<col>)` on a bare column.
/// The constructor keeps only columns with `Basic` statistics on a nullable type, so a
/// lone `IS NULL` that cannot prune does not load per-part statistics.
void collectNullPredicates(
    const ActionsDAG::Node & node,
    const ColumnsDescription & columns,
    std::vector<std::pair<String, bool>> & out)
{
    if (node.type == ActionsDAG::ActionType::INPUT)
    {
        if (auto parent = tryResolveNullMapParent(columns, node.result_name))
            out.emplace_back(*parent, true);
        return;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return;

    const auto & name = node.function_base->getName();
    if (name == "and")
    {
        for (const auto * child : node.children)
            collectNullPredicates(*child, columns, out);
        return;
    }

    if (node.children.size() == 1 && node.children.front()->type == ActionsDAG::ActionType::INPUT)
    {
        const String & arg_name = node.children.front()->result_name;
        if (name == "not")
        {
            if (auto parent = tryResolveNullMapParent(columns, arg_name))
                out.emplace_back(*parent, false);
        }
        else if (name == "isNull" || name == "isNotNull")
        {
            out.emplace_back(arg_name, name == "isNull");
        }
        return;
    }

    /// `isNull(col)` with `optimize_functions_to_subcolumns = 1` is rewritten to
    /// `<col>.null != 0`. Only 0 qualifies: a NULL-map byte only has to be non-zero
    /// to mean NULL. `= 0` is the complementary `IS NOT NULL` form.
    if (node.children.size() == 2 && (name == "equals" || name == "notEquals"))
    {
        const ActionsDAG::Node * input = nullptr;
        if (node.children[0]->type == ActionsDAG::ActionType::INPUT && isConstantZero(*node.children[1]))
            input = node.children[0];
        else if (node.children[1]->type == ActionsDAG::ActionType::INPUT && isConstantZero(*node.children[0]))
            input = node.children[1];

        if (input)
        {
            if (auto parent = tryResolveNullMapParent(columns, input->result_name))
                out.emplace_back(*parent, name == "notEquals");
        }
    }
}

/// Functions that negate a comparison, i.e. can be `true` for a `NaN` operand. `NaN` never
/// satisfies a plain comparison (`NaN < c`, `NaN = c`, ... are all `false`), so only a negation can
/// make a floating-point predicate `true` for `NaN`. These are exactly the negating entries of
/// `KeyCondition::atom_map` that produce a prunable `NOT_IN_RANGE` / `NOT_IN_SET` atom (`notLike`
/// and `notEmpty` are omitted: they apply only to String/Array, never to a floating-point column).
bool isNegatingFunction(std::string_view name)
{
    return name == "not"
        || name == "notEquals"
        || name == "notIn"
        || name == "globalNotIn"
        || name == "notNullIn"
        || name == "globalNotNullIn";
}

bool isFloatingPointColumn(const DataTypePtr & type)
{
    return type && isFloat(removeLowCardinalityAndNullable(type));
}

/// Collect floating-point columns that appear anywhere beneath a negating function in the original
/// (non-inverted) filter tree.
///
/// `MinMax`/`Basic` statistics compute min/max via `IColumn::getExtremes`, which deliberately skips
/// `NaN`. So the stored range excludes `NaN`, yet `NaN` sorts after `+inf` and satisfies negated
/// predicates such as `NOT (f < c)` or `f <> c`. Pruning a part by that range would then drop rows
/// that actually match. Statistics-based pruning is therefore disabled for such columns; the range
/// analysis stays sound for a plain comparison, where `NaN` cannot match anyway.
///
/// A positive `IN` also matches `NaN` (`SELECT nan IN (nan)` is `1`), but it needs no exclusion
/// here: the estimates are checked through a `KeyCondition` built with `require_ready_sets`, and
/// `KeyCondition::tryPrepareSetIndexForIn` declines a set atom whose elements contain a `NaN`, so
/// such a predicate becomes unknown for every range-based check at once. Excluding the column
/// instead would also forgo pruning for the common `NaN`-free set.
///
/// The traversal is intentionally conservative: once under a negation it stays under it for the whole
/// subtree, so a column may be excluded even where an even number of negations would cancel out.
/// Excluding a column only forgoes a pruning opportunity, never correctness.
void collectFloatColumnsUnderNegation(
    const ActionsDAG::Node & node,
    bool under_negation,
    NameSet & unsafe_columns,
    std::unordered_set<const ActionsDAG::Node *> & visited_under_negation,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    auto & visited_set = under_negation ? visited_under_negation : visited;
    if (!visited_set.insert(&node).second)
        return;

    if (under_negation
        && node.type == ActionsDAG::ActionType::INPUT
        && isFloatingPointColumn(node.result_type))
    {
        unsafe_columns.insert(node.result_name);
    }

    const bool child_under_negation = under_negation
        || (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base
            && isNegatingFunction(node.function_base->getName()));

    for (const auto * child : node.children)
        collectFloatColumnsUnderNegation(*child, child_under_negation, unsafe_columns, visited_under_negation, visited);
}

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : filter_dag(&filter_node_, context_, /* boolean_context */ true)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    /// Floating-point columns that a negated predicate (`NOT (f < c)`, `f <> c`, `f NOT IN (...)`)
    /// could match via `NaN`. Their min/max statistics exclude `NaN`, so pruning them is unsound.
    NameSet nan_unsafe_columns;
    {
        std::unordered_set<const ActionsDAG::Node *> visited_under_negation;
        std::unordered_set<const ActionsDAG::Node *> visited;
        collectFloatColumnsUnderNegation(filter_node_, /*under_negation=*/ false, nan_unsafe_columns, visited_under_negation, visited);
    }

    std::map<String, DataTypePtr> nullable_only_columns;

    for (const auto & name : filter_columns)
    {
        if (nan_unsafe_columns.contains(name))
            continue;

        if (const auto * col = columns.tryGet(name))
        {
            if (statisticsHasMinMaxForPartPruning(col->statistics))
            {
                stats_column_name_to_type_map[col->name] = col->type;
                useless = false;
            }
            else if (hasBasicStatsOnNullableType(*col))
            {
                nullable_only_columns[col->name] = col->type;
            }
        }
    }

    std::vector<std::pair<String, bool>> collected_null_predicates;
    collectNullPredicates(*filter_dag.predicate, columns, collected_null_predicates);
    for (const auto & pred : collected_null_predicates)
    {
        const auto * col = columns.tryGet(pred.first);
        if (!col || !hasBasicStatsOnNullableType(*col))
            continue;

        null_predicates.push_back(pred);
        used_column_names.insert(pred.first);
        useless = false;
    }

    /// Keep a nullable-only column only when it can exclude all-NULL or no-NULL parts by itself.
    for (const auto & [col_name, col_type] : nullable_only_columns)
    {
        NamesAndTypesList one_col;
        one_col.emplace_back(col_name, col_type);
        KeyCondition * key_condition = getKeyConditionForEstimates(one_col, /*record_used_columns=*/ false);
        if (!key_condition || !rejectsNullCountWitness(*key_condition, col_type))
            continue;

        stats_column_name_to_type_map[col_name] = col_type;
        used_column_names.insert(col_name);
        useless = false;
    }
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns, bool record_used_columns)
{
    const auto column_names = columns.getNames();

    auto it = key_condition_cache.find(column_names);
    if (it != key_condition_cache.end())
        return it->second.get();

    ActionsDAG actions_dag(columns);
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

    /// Pruning estimates must not run a query pipeline: only state that is already computed may be
    /// read here.
    auto new_key_condition = std::make_unique<KeyCondition>(
        filter_dag, context, column_names, expression,
        /* single_point_ */ false, /* skip_analysis_ */ false, /* require_ready_sets_ */ true);

    /// A statistic's min/max is aggregated the same way `getExtremes` is, so it skips NaN.
    /// Must run before `alwaysUnknownOrTrue()`, whose verdict this can change.
    new_key_condition->relaxAtomsOverNaNHidingColumns(columns.getTypes());

    if (new_key_condition->alwaysUnknownOrTrue())
    {
        key_condition_cache[column_names] = nullptr;
        return nullptr;
    }

    auto & cached_key_condition = key_condition_cache[column_names];
    cached_key_condition = std::move(new_key_condition);
    auto * key_condition_ptr = cached_key_condition.get();

    if (record_used_columns)
    {
        for (size_t col_idx : key_condition_ptr->getUsedColumns())
        {
            if (col_idx < column_names.size())
                used_column_names.insert(column_names[col_idx]);
        }
    }

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    for (const auto & [column, is_null] : null_predicates)
    {
        auto est_it = estimates.find(column);
        if (est_it == estimates.end())
            continue;

        const Estimate & estimate = est_it->second;
        if (!estimate.estimated_null_count.has_value() || estimate.rows_count == 0
            || *estimate.estimated_null_count > estimate.rows_count)
            continue;

        if (is_null && *estimate.estimated_null_count == 0)
            return {false, true};
        if (!is_null && *estimate.estimated_null_count == estimate.rows_count)
            return {false, true};
    }

    /// Use only columns that are both in filter and have estimates
    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        if (estimates.contains(col_name))
            columns.emplace_back(col_name, col_type);
    }

    if (columns.empty())
        return {true, true};

    KeyCondition * key_condition = getKeyConditionForEstimates(columns);
    if (!key_condition)
        return {true, true};

    Hyperrectangle hyperrectangle;
    DataTypes types;

    for (const auto & [col_name, col_type] : columns)
    {
        auto range = createRangeFromEstimate(estimates.at(col_name), isNullableOrLowCardinalityNullable(col_type));

        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else if (isNullableOrLowCardinalityNullable(col_type))
            hyperrectangle.emplace_back(Range::createWholeUniverse());
        else
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
