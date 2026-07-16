#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

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

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : filter_dag(&filter_node_, context_)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    for (const auto & name : filter_columns)
    {
        if (const auto * col = columns.tryGet(name))
        {
            if (col->statistics.types_to_desc.contains(StatisticsType::MinMax))
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

    auto new_key_condition = std::make_unique<KeyCondition>(filter_dag, context, column_names, expression);

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
    /// Filter estimates with loaded MinMax statistics.
    Estimates minmax_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.types.contains(StatisticsType::MinMax))
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
