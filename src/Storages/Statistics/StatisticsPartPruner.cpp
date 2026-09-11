#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>
#include <cstring>

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
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, const DataTypePtr & /*data_type*/, bool is_nullable)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    const std::optional<UInt64> & null_count = estimate.estimated_null_count;
    if (null_count.has_value() && *null_count > estimate.rows_count)
        return std::nullopt; /// corrupted statistics

    if (estimate.estimated_min.has_value() && estimate.estimated_max.has_value())
    {
        const Field & min_value = estimate.estimated_min.value();
        const Field & max_value = estimate.estimated_max.value();

        /// min > max is a legacy sentinel pair or corrupted statistics; only an all-NULL
        /// part of a nullable column yields a prunable range here.
        if (min_value > max_value)
        {
            if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
                return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
            return std::nullopt;
        }

        if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);

        if (!is_nullable || (null_count.has_value() && *null_count == 0))
            return Range(min_value, true, max_value, true);

        /// Nullable column that may contain NULLs: keep the right bound at the NULL sentinel.
        return Range(min_value, true, POSITIVE_INFINITY, true);
    }

    /// No min/max (non-numeric type like String/Array/Tuple/Map, or an all-NULL part):
    /// the NULL count alone can still produce a useful range for a nullable column.
    if (is_nullable && null_count.has_value())
    {
        if (*null_count == estimate.rows_count)
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        if (*null_count == 0)
            return Range::createWholeUniverseWithoutNull();
        /// Partial NULLs cannot be expressed as a single continuous Range.
        return std::nullopt;
    }

    return std::nullopt;
}

/// Returns true when a column's statistics description can produce a useful range for part
/// pruning: numeric min/max values (an explicit `MinMax` statistic, or `Basic` on a
/// numeric/temporal column), or a NULL count (`Basic` on a nullable column). Used before part
/// statistics are loaded to decide whether part pruning can be beneficial at all.
bool statisticsSupportsPartPruning(const ColumnStatisticsDescription & stats_desc)
{
    if (stats_desc.types_to_desc.contains(StatisticsType::MinMax))
        return true;
    if (stats_desc.types_to_desc.contains(StatisticsType::Basic))
        return removeLowCardinalityAndNullable(stats_desc.data_type)->isValueRepresentedByNumber()
            || isNullableOrLowCardinalityNullable(stats_desc.data_type);
    return false;
}

/// For `Nullable(T)` where `T` owns a `null` subcolumn of its own (e.g. `Nullable(JSON)`,
/// where `json.null` is the nested JSON path of type `Dynamic`), `<col>.null` resolves to
/// that real subcolumn instead of the `Nullable` null-map. Mirrors
/// `nestedTypeHasNullSubcolumn` in `FunctionToSubcolumnsPass`.
bool nestedTypeHasNullSubcolumn(const DataTypePtr & type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return nullable_type->getNestedType()->hasSubcolumn("null");
    return false;
}

/// If `name` is the `<parent>.null` null-map of a nullable column with `Basic` statistics,
/// return the parent column name. A physical column literally named `foo.null` wins, and a
/// parent whose nested type owns a `null` subcolumn (e.g. `Nullable(JSON)`) is excluded.
std::optional<String> tryResolveNullMapParent(const StorageMetadataPtr & metadata, const String & name)
{
    if (!name.ends_with(".null"))
        return std::nullopt;
    const auto & columns = metadata->getColumns();
    if (columns.tryGet(name))
        return std::nullopt; /// physical column
    String parent = name.substr(0, name.size() - strlen(".null"));
    if (const auto * parent_col = columns.tryGet(parent))
    {
        if (parent_col->statistics.types_to_desc.contains(StatisticsType::Basic)
            && isNullableOrLowCardinalityNullable(parent_col->type)
            && !nestedTypeHasNullSubcolumn(parent_col->type))
            return parent;
    }
    return std::nullopt;
}

bool hasBasicStatsOnNullableType(const ColumnDescription & col)
{
    return col.statistics.types_to_desc.contains(StatisticsType::Basic)
        && isNullableOrLowCardinalityNullable(col.type);
}

/// Collect top-level `AND` conjuncts testing a column's NULL-ness: a bare `<col>.null`
/// input (`IS NULL` rewritten by `optimize_functions_to_subcolumns`), `not(<col>.null)`,
/// or `isNull(<col>)` / `isNotNull(<col>)` on a bare column.
void collectNullPredicates(
    const ActionsDAG::Node & node,
    const StorageMetadataPtr & metadata,
    std::vector<StatisticsPartPruner::NullPredicate> & out)
{
    if (node.type == ActionsDAG::ActionType::INPUT)
    {
        if (auto parent = tryResolveNullMapParent(metadata, node.result_name))
            out.push_back({*parent, /*is_null=*/true});
        return;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return;

    const auto & name = node.function_base->getName();
    if (name == "and")
    {
        for (const auto * child : node.children)
            collectNullPredicates(*child, metadata, out);
        return;
    }

    if (node.children.size() != 1 || node.children.front()->type != ActionsDAG::ActionType::INPUT)
        return;

    const String & arg_name = node.children.front()->result_name;
    if (name == "not")
    {
        if (auto parent = tryResolveNullMapParent(metadata, arg_name))
            out.push_back({*parent, /*is_null=*/false});
    }
    else if (name == "isNull" || name == "isNotNull")
    {
        if (const auto * col = metadata->getColumns().tryGet(arg_name); col && hasBasicStatsOnNullableType(*col))
            out.push_back({arg_name, name == "isNull"});
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
/// analysis stays sound for every other (non-negated) predicate, where `NaN` cannot match anyway.
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

    for (const auto & name : filter_columns)
    {
        if (nan_unsafe_columns.contains(name))
            continue;

        if (const auto * col = columns.tryGet(name))
        {
            if (statisticsSupportsPartPruning(col->statistics))
            {
                stats_column_name_to_type_map[col->name] = col->type;
                useless = false;
            }
        }
    }

    collectNullPredicates(*filter_dag.predicate, metadata_, null_predicates);
    if (!null_predicates.empty())
        useless = false;
    for (const auto & pred : null_predicates)
        used_column_names.insert(pred.column);
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns)
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

    if (new_key_condition->alwaysUnknownOrTrue())
    {
        key_condition_cache[column_names] = nullptr;
        return nullptr;
    }

    auto * key_condition_ptr = new_key_condition.get();
    key_condition_cache[column_names] = std::move(new_key_condition);

    for (size_t col_idx : key_condition_ptr->getUsedColumns())
    {
        if (col_idx < column_names.size())
            used_column_names.insert(column_names[col_idx]);
    }

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    /// Filter to estimates that can produce a useful range: numeric min/max values or a
    /// NULL count. An all-NULL part has no min/max at all, so gating on `estimated_min`
    /// alone would silently drop the only evidence we have for such parts.
    Estimates pruning_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.estimated_min.has_value() || estimate.estimated_null_count.has_value())
            pruning_estimates[col_name] = estimate;
    }

    if (pruning_estimates.empty())
        return {true, true};

    /// `IS NULL` cannot match a part with zero NULLs; `IS NOT NULL` cannot match an all-NULL part.
    for (const auto & [column, is_null] : null_predicates)
    {
        auto est_it = pruning_estimates.find(column);
        if (est_it == pruning_estimates.end())
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
        if (pruning_estimates.contains(col_name))
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
        auto est_it = pruning_estimates.find(col_name);
        chassert(est_it != pruning_estimates.end());

        auto is_nullable_type = isNullableOrLowCardinalityNullable(col_type);
        auto range = createRangeFromEstimate(est_it->second, col_type, is_nullable_type);

        if (range.has_value())
        {
            hyperrectangle.push_back(std::move(*range));
        }
        else if (is_nullable_type)
        {
            hyperrectangle.emplace_back(Range::createWholeUniverse());
        }
        else
        {
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
