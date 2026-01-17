#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Float64 can exactly represent integers in the range [-2^53, 2^53].
static constexpr Float64 MAX_EXACT_FLOAT64 = static_cast<Float64>(1ULL << std::numeric_limits<Float64>::digits);

enum class RangeStatus : uint8_t
{
    Valid,    /// Valid range, use it for pruning
    Unknown,  /// Cannot determine range, be conservative don't prune
    Empty     /// Empty range, should prune this part
};

struct RangeWithStatus
{
    std::optional<Range> range;
    RangeStatus status = RangeStatus::Unknown;
};

/// Try to convert Float64 value to target type using strict conversion.
/// Returns the converted Field if successful, nullopt otherwise.
static std::optional<Field> tryConvertFloat64Strict(Float64 value, const DataTypePtr & data_type)
{
    static const DataTypePtr float64_type = std::make_shared<DataTypeFloat64>();
    return convertFieldToTypeStrict(Field(value), *float64_type, *data_type);
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

static RangeWithStatus createRangeFromEstimate(const Estimate & est, const DataTypePtr & data_type)
{
    if (!est.estimated_min.has_value() || !est.estimated_max.has_value())
        return {std::nullopt, RangeStatus::Unknown};

    Float64 min_value = est.estimated_min.value();
    Float64 max_value = est.estimated_max.value();

    /// If min > max, the part has all nulls, should be pruned
    if (min_value > max_value)
        return {std::nullopt, RangeStatus::Empty};

    WhichDataType which(data_type);
    bool needs_precision_handling = which.isInt64() || which.isInt128() || which.isInt256()
                                 || which.isUInt64() || which.isUInt128() || which.isUInt256()
                                 || which.isDecimal();

    if (!needs_precision_handling)
    {
        /// For types that don't have precision issues, convert Float64 to appropriate integer type first.
        /// convertFieldToType doesn't support direct Float64 -> Date/DateTime conversion.
        Field min_src = which.isFloat() ? Field(min_value) : Field(static_cast<Int64>(min_value));
        Field max_src = which.isFloat() ? Field(max_value) : Field(static_cast<Int64>(max_value));

        Field min_field = convertFieldToType(min_src, *data_type);
        Field max_field = convertFieldToType(max_src, *data_type);

        if (min_field.isNull() || max_field.isNull())
            return {std::nullopt, RangeStatus::Unknown};

        return {Range(min_field, true, max_field, true), RangeStatus::Valid};
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
        return {std::nullopt, RangeStatus::Unknown};
    }
    else if (!min_field_opt.has_value())
    {
        /// Only max is valid: range is (-inf, max]
        return {Range::createRightBounded(max_field_opt.value(), true), RangeStatus::Valid};
    }
    else if (!max_field_opt.has_value())
    {
        /// Only min is valid: range is [min, +inf)
        return {Range::createLeftBounded(min_field_opt.value(), true), RangeStatus::Valid};
    }
    else
    {
        /// Both valid: range is [min, max]
        return {Range(min_field_opt.value(), true, max_field_opt.value(), true), RangeStatus::Valid};
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
            hyperrectangle.emplace_back(Range::createWholeUniverse());
            types.push_back(col_type);
            continue;
        }

        auto [range, status] = createRangeFromEstimate(est_it->second, col_type);
        switch (status)
        {
            case RangeStatus::Valid:
                hyperrectangle.push_back(std::move(*range));
                types.push_back(col_type);
                break;
            case RangeStatus::Unknown:
                hyperrectangle.emplace_back(Range::createWholeUniverse());
                types.push_back(col_type);
                break;
            case RangeStatus::Empty:
                return {false, true};
        }
    }

    return key_condition.checkInHyperrectangle(hyperrectangle, types);
}

}
