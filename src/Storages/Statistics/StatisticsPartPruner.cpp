#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
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

/// Get boundary fields for min/max values when Float64 precision is exceeded.
/// For min: if value >= 2^53, use 2^53 as conservative lower bound.
/// For max: if value <= -2^53, use -2^53 as conservative upper bound.
/// Returns a pair of optional fields: (min_boundary, max_boundary).
static std::pair<std::optional<Field>, std::optional<Field>> getBoundaryFields(
    Float64 min_value, Float64 max_value, const DataTypePtr & data_type)
{
    std::optional<Field> min_boundary;
    std::optional<Field> max_boundary;

    WhichDataType which(data_type);

    /// For min: if value >= 2^53, use 2^53 as conservative lower bound
    if (min_value >= MAX_EXACT_FLOAT64)
    {
        if (which.isUInt())
            min_boundary = convertFieldToType(Field(static_cast<UInt64>(MAX_EXACT_FLOAT64)), *data_type);
        else
            min_boundary = convertFieldToType(Field(static_cast<Int64>(MAX_EXACT_FLOAT64)), *data_type);
    }

    /// For max: if value <= -2^53, use -2^53 as conservative upper bound
    if (max_value <= -MAX_EXACT_FLOAT64)
    {
        if (!which.isUInt())
            max_boundary = convertFieldToType(Field(static_cast<Int64>(-MAX_EXACT_FLOAT64)), *data_type);
    }

    return {min_boundary, max_boundary};
}

/// Check if Float64 value exceeds the exact representable range [-2^53, 2^53].
static bool exceedsFloat64PrecisionRange(Float64 value)
{
    return value >= MAX_EXACT_FLOAT64 || value <= -MAX_EXACT_FLOAT64;
}

/// Create a Range from statistics estimate for use in part pruning.
/// For nullable columns, the range is conservatively extended to POSITIVE_INFINITY
/// This is a conservative approach because StatisticsMinMax loses information about whether the column actually contains NULL values.
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
        chassert(max_value == std::numeric_limits<Float64>::min());
        return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
    }

    DataTypePtr inner_data_type = removeLowCardinalityAndNullable(data_type);
    WhichDataType which(inner_data_type);
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

        /// For nullable columns, extend max to POSITIVE_INFINITY to include NULL values.
        if (is_nullable)
            return Range(min_field, true, POSITIVE_INFINITY, true);

        return Range(min_field, true, max_field, true);
    }

    /// For types that may exceed Float64 precision, check precision range first.
    /// If the value exceeds Float64 precision range, use conservative boundary values directly
    /// since tryConvertFloat64Strict doesn't detect precision loss for integer types.
    std::optional<Field> min_field_opt;
    std::optional<Field> max_field_opt;

    bool min_exceeds_precision = exceedsFloat64PrecisionRange(min_value);
    bool max_exceeds_precision = exceedsFloat64PrecisionRange(max_value);

    if (min_exceeds_precision || max_exceeds_precision)
    {
        auto [min_boundary, max_boundary] = getBoundaryFields(min_value, max_value, data_type);
        if (min_exceeds_precision)
            min_field_opt = min_boundary;
        else
            min_field_opt = tryConvertFloat64Strict(min_value, data_type);

        if (max_exceeds_precision)
            max_field_opt = max_boundary;
        else
            max_field_opt = tryConvertFloat64Strict(max_value, data_type);
    }
    else
    {
        min_field_opt = tryConvertFloat64Strict(min_value, data_type);
        max_field_opt = tryConvertFloat64Strict(max_value, data_type);
    }

    if (!min_field_opt.has_value() && !max_field_opt.has_value())
    {
        /// Both conversions failed, cannot determine any useful bound
        return std::nullopt;
    }
    else if (!min_field_opt.has_value())
    {
        /// Only max is valid: range is (-inf, max] or (-inf, +inf] for nullable
        if (is_nullable)
            return Range::createWholeUniverse();
        return Range::createRightBounded(max_field_opt.value(), true);
    }
    else if (!max_field_opt.has_value())
    {
        /// Only min is valid: range is [min, +inf)
        /// For nullable, this already includes POSITIVE_INFINITY with with_null=true
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

StatisticsPartPruner::StatisticsPartPruner(
    const StorageMetadataPtr & metadata,
    const ActionsDAG::Node * filter_node,
    ContextPtr context)
{
    if (!filter_node || !metadata)
        return;

    /// Collect columns that have MinMax statistics
    const auto & columns = metadata->getColumns();
    for (const auto & column : columns)
    {
        if (column.statistics.types_to_desc.contains(StatisticsType::MinMax))
            stats_column_name_to_type_map[column.name] = column.type;
    }

    if (stats_column_name_to_type_map.empty())
        return;

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
    key_condition.emplace(filter_dag, context, stats_column_names, expression);

    if (key_condition->alwaysUnknownOrTrue())
        return;

    for (size_t col_idx : key_condition->getUsedColumns())
        if (col_idx < stats_column_names.size())
            used_column_names.push_back(stats_column_names[col_idx]);

    useless = false;
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

        auto range = createRangeFromEstimate(est_it->second, col_type, isNullableOrLowCardinalityNullable(col_type));
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

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
