#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <base/defines.h>

namespace DB
{

/// Float64 can exactly represent integers in the range [-2^53, 2^53].
static constexpr Float64 MAX_EXACT_FLOAT64 = static_cast<Float64>(1ULL << std::numeric_limits<Float64>::digits);


/// Try to convert Float64 value to target type using strict conversion.
/// Returns the converted Field if successful, nullopt otherwise.
static std::optional<Field> tryConvertFloat64Strict(Float64 value, const DataTypePtr & data_type)
{
    static const DataTypePtr float64_type = std::make_shared<DataTypeFloat64>();
    try
    {
        return convertFieldToTypeStrict(Field(value), *float64_type, *data_type);
    }
    catch (...)
    {
        /// convertFieldToTypeStrict may throw when value exceeds target type's range (e.g., Decimal overflow)
        return std::nullopt;
    }
}

/// Get boundary field for min value when Float64 precision is exceeded.
/// If value >= 2^53, use 2^53 as conservative lower bound.
/// If value <= -2^53, we cannot determine a safe lower bound, return nullopt.
static std::optional<Field> getMinBoundaryField(Float64 value, const DataTypePtr & data_type)
{
    if (value >= MAX_EXACT_FLOAT64)
    {
        WhichDataType which(data_type);
        if (which.isUInt())
            return convertFieldToType(Field(static_cast<UInt64>(MAX_EXACT_FLOAT64)), *data_type);
        else
            return convertFieldToType(Field(static_cast<Int64>(MAX_EXACT_FLOAT64)), *data_type);
    }

    return std::nullopt;
}

/// Get boundary field for max value when Float64 precision is exceeded.
/// If value <= -2^53, use -2^53 as conservative upper bound.
/// If value >= 2^53, we cannot determine a safe upper bound, return nullopt.
static std::optional<Field> getMaxBoundaryField(Float64 value, const DataTypePtr & data_type)
{
    if (value <= -MAX_EXACT_FLOAT64)
    {
        WhichDataType which(data_type);
        if (which.isUInt())
            return std::nullopt;
        else
            return convertFieldToType(Field(static_cast<Int64>(-MAX_EXACT_FLOAT64)), *data_type);
    }

    return std::nullopt;
}

/// Check if Float64 value exceeds the exact representable range [-2^53, 2^53].
static bool exceedsFloat64PrecisionRange(Float64 value)
{
    return value >= MAX_EXACT_FLOAT64 || value <= -MAX_EXACT_FLOAT64;
}

static std::optional<Range> createRangeFromEstimate(const Estimate & est, const DataTypePtr & data_type)
{
    if (!est.estimated_min.has_value() || !est.estimated_max.has_value())
        return std::nullopt;

    Float64 min_value = est.estimated_min.value();
    Float64 max_value = est.estimated_max.value();

    /// If min > max, the part likely has all NULL values, create a range containing only POSITIVE_INFINITY.
    if (min_value > max_value)
    {
        chassert(min_value == std::numeric_limits<Float64>::max());
        chassert(max_value == std::numeric_limits<Float64>::min());
        return Range(POSITIVE_INFINITY);
    }

    WhichDataType which(data_type);
    bool needs_precision_handling = which.isInt64() || which.isInt128() || which.isInt256()
                                 || which.isUInt64() || which.isUInt128() || which.isUInt256()
                                 || which.isDecimal();

    if (!needs_precision_handling)
    {
        /// Different types require different Field types:
        /// - Float types: use Float64 directly
        /// - DateTime, IPv4: require UInt64
        /// - Other integer types: Int64 works
        Field min_src;
        Field max_src;
        if (which.isFloat())
        {
            min_src = Field(min_value);
            max_src = Field(max_value);
        }
        else if (which.isDateTime() || which.isIPv4())
        {
            /// DateTime and IPv4 require UInt64, negative values are invalid
            if (min_value < 0 || max_value < 0)
                return std::nullopt;

            min_src = Field(static_cast<UInt64>(min_value));
            max_src = Field(static_cast<UInt64>(max_value));
        }
        else
        {
            /// Int8/16/32, UInt8/16/32, Date, Date32, Time, Enum - Int64 works for all
            min_src = Field(static_cast<Int64>(min_value));
            max_src = Field(static_cast<Int64>(max_value));
        }

        Field min_field = convertFieldToType(min_src, *data_type);
        Field max_field = convertFieldToType(max_src, *data_type);

        if (min_field.isNull() || max_field.isNull())
            return std::nullopt;

        return Range(min_field, true, max_field, true);
    }

    /// For types that may exceed Float64 precision, use strict conversion first
    std::optional<Field> min_field_opt = tryConvertFloat64Strict(min_value, data_type);
    std::optional<Field> max_field_opt = tryConvertFloat64Strict(max_value, data_type);

    /// If strict conversion failed, try to use boundary values for out-of-precision-range cases
    if (!min_field_opt.has_value() && exceedsFloat64PrecisionRange(min_value))
        min_field_opt = getMinBoundaryField(min_value, data_type);

    if (!max_field_opt.has_value() && exceedsFloat64PrecisionRange(max_value))
        max_field_opt = getMaxBoundaryField(max_value, data_type);

    if (!min_field_opt.has_value() && !max_field_opt.has_value())
    {
        /// Both conversions failed, cannot determine any useful bound
        return std::nullopt;
    }
    else if (!min_field_opt.has_value())
    {
        /// Only max is valid: range is (-inf, max]
        return Range::createRightBounded(max_field_opt.value(), true);
    }
    else if (!max_field_opt.has_value())
    {
        /// Only min is valid: range is [min, +inf)
        return Range::createLeftBounded(min_field_opt.value(), true);
    }
    else
    {
        /// Both valid: range is [min, max]
        return Range(min_field_opt.value(), true, max_field_opt.value(), true);
    }
}

StatisticsPartPruner::StatisticsPartPruner(
    KeyCondition key_condition_,
    std::map<String, DataTypePtr> stats_column_name_to_type_map_,
    std::vector<String> used_column_names_)
    : key_condition(std::move(key_condition_))
    , stats_column_name_to_type_map(std::move(stats_column_name_to_type_map_))
    , used_column_names(std::move(used_column_names_))
{
}

std::optional<StatisticsPartPruner> StatisticsPartPruner::create(
    const StorageMetadataPtr & metadata,
    const ActionsDAG::Node * filter_node,
    ContextPtr context)
{
    if (!filter_node || !metadata)
        return std::nullopt;

    /// Collect columns that have MinMax statistics
    std::map<String, DataTypePtr> stats_column_name_to_type_map;
    const auto & columns = metadata->getColumns();
    for (const auto & column : columns)
    {
        if (column.statistics.types_to_desc.contains(StatisticsType::MinMax))
            stats_column_name_to_type_map[column.name] = column.type;
    }

    if (stats_column_name_to_type_map.empty())
        return std::nullopt;

    /// Build column names in sorted order
    Names stats_column_names;
    for (const auto & [name, type] : stats_column_name_to_type_map)
        stats_column_names.push_back(name);

    NamesAndTypesList inputs_list;
    for (const auto & [name, type] : stats_column_name_to_type_map)
        inputs_list.emplace_back(name, type);

    ActionsDAG actions_dag(inputs_list);
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

    ActionsDAGWithInversionPushDown filter_dag(filter_node, context);
    KeyCondition key_condition(filter_dag, context, stats_column_names, expression);

    if (key_condition.alwaysUnknownOrTrue())
        return std::nullopt;

    std::vector<String> used_column_names;
    for (size_t col_idx : key_condition.getUsedColumns())
        if (col_idx < stats_column_names.size())
            used_column_names.push_back(stats_column_names[col_idx]);

    return StatisticsPartPruner(std::move(key_condition), std::move(stats_column_name_to_type_map), std::move(used_column_names));
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates) const
{
    Hyperrectangle hyperrectangle;
    DataTypes types;

    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        auto est_it = estimates.find(col_name);
        if (est_it == estimates.end())
        {
            if (isNullableOrLowCardinalityNullable(col_type))
                hyperrectangle.emplace_back(Range::createWholeUniverse());
            else
                hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
            types.push_back(col_type);
            continue;
        }

        auto range = createRangeFromEstimate(est_it->second, col_type);
        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else
        {
            if (isNullableOrLowCardinalityNullable(col_type))
                hyperrectangle.emplace_back(Range::createWholeUniverse());
            else
                hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        types.push_back(col_type);
    }

    return key_condition.checkInHyperrectangle(hyperrectangle, types);
}

}
