#include <Storages/ObjectStorage/DataLakes/Common/DataLakeColumnStatistics.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/Statistics/RelationProfile.h>
#include <Storages/Statistics/Statistics.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace DB
{

namespace
{

/// Whether `max - min + 1` of the values bounds the number of distinct values.
bool hasCountableValues(const IDataType & type)
{
    const WhichDataType which(type);
    return which.isNativeInteger() || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64();
}

/// `max - min + 1` over the raw values, saturating; nullopt for a kind of value that is not counted.
std::optional<UInt64> valueRangeWidth(const Field & min_value, const Field & max_value)
{
    if (min_value.getType() != max_value.getType())
        return std::nullopt;

    UInt64 difference = 0;
    switch (min_value.getType())
    {
        case Field::Types::Int64:
            difference = static_cast<UInt64>(max_value.safeGet<Int64>()) - static_cast<UInt64>(min_value.safeGet<Int64>());
            break;
        case Field::Types::UInt64:
            difference = max_value.safeGet<UInt64>() - min_value.safeGet<UInt64>();
            break;
        case Field::Types::Bool:
            difference = static_cast<UInt64>(max_value.safeGet<bool>()) - static_cast<UInt64>(min_value.safeGet<bool>());
            break;
        case Field::Types::Decimal64:
            difference = static_cast<UInt64>(max_value.safeGet<DecimalField<Decimal64>>().getValue().value)
                - static_cast<UInt64>(min_value.safeGet<DecimalField<Decimal64>>().getValue().value);
            break;
        default:
            return std::nullopt;
    }
    return difference == std::numeric_limits<UInt64>::max() ? difference : difference + 1;
}

}

void DataLakeColumnStatistics::ColumnMetrics::setIdentityPartitionValue(Field value, UInt64 rows, bool tracks_min_max)
{
    if (!nulls)
        nulls = value.isNull() ? rows : 0;
    if (tracks_min_max && min_value.isNull() && !value.isNull())
    {
        min_value = value;
        max_value = value;
    }
    identity_partition_value = std::move(value);
}

DataLakeColumnStatistics::DataLakeColumnStatistics(ContextPtr context)
    : statistics_builder(std::move(context))
{
}

bool DataLakeColumnStatistics::canBeTarget(const IDataType & nested_type)
{
    const auto type_id = nested_type.getTypeId();
    return type_id != TypeIndex::Tuple && type_id != TypeIndex::Map && type_id != TypeIndex::Array && type_id != TypeIndex::Variant
        && type_id != TypeIndex::Nothing && type_id != TypeIndex::Dynamic;
}

void DataLakeColumnStatistics::addTarget(Target target)
{
    targets.push_back(std::move(target));
    distinct_values_inputs.emplace_back();
}

void DataLakeColumnStatistics::addFile(UInt64 rows, const std::vector<ColumnMetrics> & file_metrics)
{
    total_rows += rows;
    if (targets.empty())
        return;

    chassert(file_metrics.size() == targets.size());
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & metrics = file_metrics[i];
        auto & inputs = distinct_values_inputs[i];
        if (!metrics.identity_partition_value)
            inputs.identity_partition_known = false;
        else if (!metrics.identity_partition_value->isNull())
            inputs.identity_partition_values.insert(*metrics.identity_partition_value);

        /// A file without the NULL count and the bounds adds nothing, as a MergeTree part without the statistic; with the
        /// bounds alone its NULL count is taken as 0.
        if (!metrics.nulls && metrics.min_value.isNull())
            continue;
        statistics_builder.addStatistics(
            targets[i].name,
            ColumnStatistics::createBasicFromSummary(
                targets[i].storage_type, rows, metrics.min_value, metrics.max_value, metrics.nulls.value_or(0)));
    }
}

std::optional<UInt64> DataLakeColumnStatistics::estimateDistinctValues(
    const Target & target, const DistinctValuesInputs & inputs, const ColumnStats & column, UInt64 rows)
{
    /// `value_or`: the optimizer may call `llround` before checking the optional, and MSan reports the empty value it reads.
    const UInt64 nulls = static_cast<UInt64>(std::llround(column.null_fraction.value_or(0) * static_cast<Float64>(rows)));
    const UInt64 non_null_rows = rows - std::min(nulls, rows);

    std::optional<UInt64> range_width;
    if (column.min_value && column.max_value && hasCountableValues(*target.nested_type))
        range_width = valueRangeWidth(*column.min_value, *column.max_value);

    UInt64 num_distinct_values = 1;
    if (non_null_rows == 0)
        num_distinct_values = 1;
    else if (inputs.identity_partition_known)
        num_distinct_values = inputs.identity_partition_values.size();
    /// As MergeTree interpolates ranges between min and max, the values are taken to fill the range, so a column with
    /// gaps looks more selective than it is. A range above the rows (sparse keys, timestamps) counts nothing.
    else if (range_width && *range_width <= non_null_rows)
        num_distinct_values = *range_width;
    else if (target.table_distinct_values)
        num_distinct_values = *target.table_distinct_values;
    else
        return std::nullopt;

    /// A distinct count excludes NULL, as for MergeTree statistics, and 0 would make a join a cross product.
    return std::clamp<UInt64>(num_distinct_values, 1, std::max<UInt64>(non_null_rows, 1));
}

void DataLakeColumnStatistics::subtractDeletedRows(UInt64 rows)
{
    chassert(rows <= total_rows);
    total_rows -= std::min(rows, total_rows);
}

void DataLakeColumnStatistics::finalize(DataLakeReadEstimate & estimate)
{
    estimate.rows = total_rows;

    /// Selectivities are shares of these rows.
    statistics_builder.incrementRowCount(total_rows);
    const auto estimator = statistics_builder.getEstimator();
    if (!estimator)
        return;

    /// The merged bounds and NULL counts the numbers of distinct values come from.
    const auto merged = estimator->estimateRelationProfile();
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto it = merged.column_stats.find(targets[i].name);
        if (it == merged.column_stats.end())
            continue;
        /// Without one, the estimator keeps its own guess, as for MergeTree without a `uniq` statistic.
        if (const auto distinct_values = estimateDistinctValues(targets[i], distinct_values_inputs[i], it->second, total_rows))
            statistics_builder.setCardinality(targets[i].name, *distinct_values);
    }
    estimate.estimator = estimator;
}

}
