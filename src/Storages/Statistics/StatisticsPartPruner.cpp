#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

namespace DB
{

/// Float64 (IEEE 754 double precision) can exactly represent at most 15 significant decimal digits.
static constexpr UInt32 MAX_FLOAT64_DECIMAL_PRECISION = std::numeric_limits<Float64>::digits10;

/// Float64 can exactly represent integers in the range [-2^53, 2^53].
static constexpr Float64 MAX_EXACT_FLOAT64_INTEGER = static_cast<Float64>(1ULL << std::numeric_limits<Float64>::digits);

static UInt32 getDecimalPrecisionOrZero(const DataTypePtr & data_type)
{
    const auto unwrapped = removeLowCardinalityAndNullable(data_type);
    WhichDataType which(unwrapped);
    if (!which.isDecimal())
        return 0;
    return getDecimalPrecision(*unwrapped);
}

static std::optional<Field> tryConvertFloat64(Float64 value, const DataTypePtr & data_type)
{
    try
    {
        static const DataTypePtr float64_type = std::make_shared<DataTypeFloat64>();

        auto float64_column = float64_type->createColumn();
        float64_column->insert(Field(value));

        ColumnWithTypeAndName src_column(std::move(float64_column), float64_type, "");
        auto unwrapped_type = removeLowCardinalityAndNullable(data_type);
        ColumnPtr casted_column = castColumnAccurate(src_column, unwrapped_type);

        if (!casted_column || casted_column->empty())
            return std::nullopt;

        Field result_field;
        casted_column->get(0, result_field);
        return result_field;
    }
    catch (...)
    {
        /// castColumnAccurate throws on conversion failure (overflow, etc.)
        return std::nullopt;
    }
}

/// Create a Range from statistics estimate for use in part pruning.
///
/// Statistics may have already lost origin info during collection:
///   For Decimal with precision > 15
///   For Int64/UInt64 and BigInt with values >= 2^53
///   For Nullable
/// So, the Range is conservatively extended.
static std::optional<Range> createRangeFromEstimate(const Estimate & est, const DataTypePtr & data_type, bool is_nullable)
{
    if (!est.estimated_min.has_value() || !est.estimated_max.has_value())
        return std::nullopt;

    Float64 min_value = est.estimated_min.value();
    Float64 max_value = est.estimated_max.value();

    /// If min > max, the part has all NULL values.
    if (min_value > max_value)
    {
        chassert(min_value == std::numeric_limits<Float64>::max());
        chassert(max_value == std::numeric_limits<Float64>::lowest());
        return Range(POSITIVE_INFINITY);
    }

    /// For Decimal, check if precision > 15.
    bool decimal_exceeds_precision = getDecimalPrecisionOrZero(data_type) > MAX_FLOAT64_DECIMAL_PRECISION;

    /// For Int, check if values exceed Float64's exact integer range.
    bool min_exceeds_precision = std::abs(min_value) >= MAX_EXACT_FLOAT64_INTEGER;
    bool max_exceeds_precision = std::abs(max_value) >= MAX_EXACT_FLOAT64_INTEGER;

    std::optional<Field> min_field_opt;
    std::optional<Field> max_field_opt;

    if (!min_exceeds_precision && !decimal_exceeds_precision)
        min_field_opt = tryConvertFloat64(min_value, data_type);

    if (!max_exceeds_precision && !decimal_exceeds_precision)
        max_field_opt = tryConvertFloat64(max_value, data_type);

    /// Build the range based on which boundaries are valid.
    if (!min_field_opt.has_value() && !max_field_opt.has_value())
    {
        /// Both boundaries are invalid - use whole universe
        if (is_nullable)
            return Range::createWholeUniverse();
        return Range::createWholeUniverseWithoutNull();
    }
    else if (!min_field_opt.has_value())
    {
        return Range::createRightBounded(max_field_opt.value(), true, is_nullable);
    }
    else if (!max_field_opt.has_value())
    {
        return Range::createLeftBounded(min_field_opt.value(), true, is_nullable);
    }
    else
    {
        /// Both valid: range is [min, max] or [min, +inf] for nullable
        if (is_nullable)
            return Range(min_field_opt.value(), true, POSITIVE_INFINITY, true);
        return Range(min_field_opt.value(), true, max_field_opt.value(), true);
    }
}

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node * filter_node_, ContextPtr context_)
    : filter_dag(filter_node_, context_)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    for (const auto & col : columns)
    {
        if (col.statistics.types_to_desc.contains(StatisticsType::MinMax))
        {
            if (std::find(filter_columns.begin(), filter_columns.end(), col.name) != filter_columns.end())
            {
                stats_column_name_to_type_map[col.name] = col.type;
                useless = false;
            }
        }
    }
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns) const
{
    std::vector<String> column_names;
    for (const auto & [name, _] : columns)
        column_names.push_back(name);

    auto it = key_condition_cache.find(column_names);
    if (it != key_condition_cache.end())
        return it->second.get();

    ActionsDAG actions_dag(columns);
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

    auto new_key_condition = std::make_unique<KeyCondition>(filter_dag, context, column_names, expression);

    if (new_key_condition->alwaysUnknownOrTrue())
        return nullptr;

    auto * key_condition_ptr = new_key_condition.get();
    key_condition_cache[column_names] = std::move(new_key_condition);

    for (size_t col_idx : key_condition_ptr->getUsedColumns())
    {
        if (col_idx < column_names.size())
        {
            const auto & name = column_names[col_idx];
            if (std::find(used_column_names.begin(), used_column_names.end(), name) == used_column_names.end())
                used_column_names.push_back(name);
        }
    }
    std::sort(used_column_names.begin(), used_column_names.end());

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates) const
{
    /// Filter estimates with loaded MinMax statistics.
    Estimates minmax_estimates;
    for (const auto & [col_name, est] : estimates)
    {
        if (est.types.contains(StatisticsType::MinMax))
            minmax_estimates[col_name] = est;
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
