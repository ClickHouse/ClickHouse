#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

#include <unordered_set>

namespace DB
{

namespace
{

/// Create a Range from statistics estimate for use in part pruning.
/// MinMax statistics now store typed Field values, so we can directly construct Range
/// without lossy Float64 conversions.
///
/// Returns std::nullopt when statistics are unavailable or corrupted,
/// causing the caller to fall back to a whole-universe Range (no pruning).
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, const DataTypePtr & /*data_type*/, bool is_nullable)
{
    if (!estimate.estimated_min.has_value() || !estimate.estimated_max.has_value())
        return std::nullopt;

    const Field & min_value = estimate.estimated_min.value();
    const Field & max_value = estimate.estimated_max.value();

    auto make_whole_universe = [is_nullable]() -> Range
    {
        if (is_nullable)
            return Range::createWholeUniverse();
        return Range::createWholeUniverseWithoutNull();
    };

    /// min > max indicates either an all-NULL part (sentinel pair) or corrupted statistics.
    /// Return whole-universe to avoid incorrect pruning.
    if (min_value > max_value)
        return make_whole_universe();

    /// For nullable columns, extend the right bound to POSITIVE_INFINITY
    /// because statistics don't track whether NULL values exist in the part.
    if (is_nullable)
        return Range(min_value, true, POSITIVE_INFINITY, true);

    return Range(min_value, true, max_value, true);
}

/// Returns true when a column's statistics description is expected to produce numeric
/// min/max values. Either an explicit `MinMax` statistic is declared, or a `Basic`
/// statistic on a numeric/temporal column (the only types for which `Basic` populates
/// min/max). Used before part statistics are loaded to decide whether part pruning can
/// be beneficial at all.
bool statisticsHasMinMax(const ColumnStatisticsDescription & stats_desc)
{
    if (stats_desc.types_to_desc.contains(StatisticsType::MinMax))
        return true;
    if (stats_desc.types_to_desc.contains(StatisticsType::Basic))
        return removeLowCardinalityAndNullable(stats_desc.data_type)->isValueRepresentedByNumber();
    return false;
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
            if (statisticsHasMinMax(col->statistics))
            {
                stats_column_name_to_type_map[col->name] = col->type;
                useless = false;
            }
        }
    }
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
    /// Filter to estimates that actually carry numeric min/max values. Both `MinMax` and
    /// `Basic` (on numeric/temporal types) populate `estimated_min`/`estimated_max`; for
    /// other types (Array, Tuple, Map, ...) `Basic` leaves them as `nullopt`. Checking
    /// `estimated_min.has_value()` is the authoritative gate regardless of statistic type.
    Estimates minmax_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.estimated_min.has_value())
            minmax_estimates[col_name] = estimate;
    }

    if (minmax_estimates.empty())
        return {true, true};

    /// Use only columns that are both in filter and have estimates
    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        if (minmax_estimates.contains(col_name))
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
        auto est_it = minmax_estimates.find(col_name);
        chassert(est_it != minmax_estimates.end());

        auto is_nullable_type = isNullableOrLowCardinalityNullable(col_type);
        auto range = createRangeFromEstimate(est_it->second, col_type, is_nullable_type);

        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else
        {
            /// For columns that cannot create Range, create dummy Ranges.
            if (is_nullable_type)
                hyperrectangle.emplace_back(Range::createWholeUniverse());
            else
                hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
