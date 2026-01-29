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

    auto make_whole_universe = [is_nullable]() -> Range
    {
        if (is_nullable)
            return Range::createWholeUniverse();
        return Range::createWholeUniverseWithoutNull();
    };

    /// If min > max, the part has all NULL values.
    /// We use POSITIVE_INFINITY to represent NULL, following the NULLS_LAST approach.
    /// This is consistent with MergeTree's NULL handling:
    /// - MergeTreeIndexMinMax.cpp (minmax_idx deserialization)
    /// - IMergeTreeDataPart.cpp (partition minmax_idx loading)
    /// - PartitionPruner.cpp (partition value handling)
    /// The NULLS_LAST principle applies to ORDER BY and primary key sorting (see docs/en/engines/table-engines/mergetree-family/mergetree.md:203).
    if (min_value > max_value)
    {
        chassert(min_value == std::numeric_limits<Float64>::max());
        chassert(max_value == std::numeric_limits<Float64>::lowest());
        return Range(POSITIVE_INFINITY);
    }

    /// For Decimal, check if precision > 15 - skip statistics entirely.
    if (getDecimalPrecisionOrZero(data_type) > MAX_FLOAT64_DECIMAL_PRECISION)
        return make_whole_universe();

    /// For Int, handle values outside Float64's exact integer range [-2^53, 2^53].
    /// When |value| > 2^53, Float64 cannot represent the exact integer, so we use
    /// the boundary value 2^53 (or -2^53) as a safe conservative bound.
    ///
    /// Cases:
    /// 1. max <= -2^53: all data in negative overflow region -> (-inf, -2^53]
    /// 2. min >= 2^53: all data in positive overflow region -> [2^53, +inf)
    /// 3. min <= -2^53: left bound unreliable -> (-inf
    /// 4. max >= 2^53: right bound unreliable -> +inf)
    bool min_in_negative_overflow = min_value <= -MAX_EXACT_FLOAT64_INTEGER;
    bool max_in_negative_overflow = max_value <= -MAX_EXACT_FLOAT64_INTEGER;
    bool min_in_positive_overflow = min_value >= MAX_EXACT_FLOAT64_INTEGER;
    bool max_in_positive_overflow = max_value >= MAX_EXACT_FLOAT64_INTEGER;

    /// Case 1: max <= -2^53, use (-inf, -2^53]
    if (max_in_negative_overflow)
    {
        auto boundary = tryConvertFloat64(-MAX_EXACT_FLOAT64_INTEGER, data_type);
        if (!boundary.has_value())
            return make_whole_universe();
        return Range::createRightBounded(boundary.value(), true, is_nullable);
    }

    /// Case 2: min >= 2^53, use [2^53, +inf)
    if (min_in_positive_overflow)
    {
        auto boundary = tryConvertFloat64(MAX_EXACT_FLOAT64_INTEGER, data_type);
        if (!boundary.has_value())
            return make_whole_universe();
        return Range::createLeftBounded(boundary.value(), true, is_nullable);
    }

    std::optional<Field> left_bound;
    std::optional<Field> right_bound;

    /// Case 3: min <= -2^53, left bound is -inf
    if (!min_in_negative_overflow)
        left_bound = tryConvertFloat64(min_value, data_type);

    /// Case 4: max >= 2^53, right bound is +inf
    if (!max_in_positive_overflow)
        right_bound = tryConvertFloat64(max_value, data_type);

    /// Build the range based on which boundaries are valid.
    if (!left_bound.has_value() && !right_bound.has_value())
    {
        return make_whole_universe();
    }
    else if (!left_bound.has_value())
    {
        return Range::createRightBounded(right_bound.value(), true, is_nullable);
    }
    else if (!right_bound.has_value())
    {
        return Range::createLeftBounded(left_bound.value(), true, is_nullable);
    }
    else
    {
        /// Both valid: range is [min, max] or [min, +inf] for nullable
        if (is_nullable)
            return Range(left_bound.value(), true, POSITIVE_INFINITY, true);
        return Range(left_bound.value(), true, right_bound.value(), true);
    }
}

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
        return nullptr;

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
