#include <Storages/ObjectStorage/DataLakes/Common/DataLakeColumnStatistics.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/Statistics/RelationProfile.h>
#include <Storages/Statistics/Statistics.h>
#include <base/arithmeticOverflow.h>

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

    /// The bounds are written by any writer: a minimum above the maximum says nothing about the values. Otherwise the
    /// difference of the signed values fits in `UInt64`, computed modulo 2^64.
    auto width = [](auto max, auto min) -> std::optional<UInt64>
    {
        if (max < min)
            return std::nullopt;
        return common::subIgnoreOverflow(static_cast<UInt64>(max), static_cast<UInt64>(min));
    };

    std::optional<UInt64> difference;
    switch (min_value.getType())
    {
        case Field::Types::Int64:
            difference = width(max_value.safeGet<Int64>(), min_value.safeGet<Int64>());
            break;
        case Field::Types::UInt64:
            difference = width(max_value.safeGet<UInt64>(), min_value.safeGet<UInt64>());
            break;
        case Field::Types::Bool:
            difference = width(max_value.safeGet<bool>(), min_value.safeGet<bool>());
            break;
        case Field::Types::Decimal64:
            difference = width(max_value.safeGet<DecimalField<Decimal64>>().getValue().value,
                min_value.safeGet<DecimalField<Decimal64>>().getValue().value);
            break;
        default:
            return std::nullopt;
    }
    if (!difference)
        return std::nullopt;
    return *difference == std::numeric_limits<UInt64>::max() ? *difference : *difference + 1;
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

        /// A file without the NULL count and the bounds adds nothing, as a MergeTree part without the statistic. With the
        /// bounds alone, the merged NULL count misses its NULLs, which the statistics record (see
        /// `StatisticsBasic::nullCountCoversAllRows`), so that the NULL fraction is unknown while the bounds stay valid.
        /// With the NULL count alone, the merged bounds miss its non-NULL values, which the statistics record as well (see
        /// `StatisticsBasic::minMaxCoversAllValues`), so that neither the bounds nor a number of distinct values derived
        /// from them give a guarantee.
        if (!metrics.nulls && metrics.min_value.isNull())
            continue;
        statistics_builder.addStatistics(
            targets[i].name,
            ColumnStatistics::createBasicFromSummary(targets[i].storage_type, rows, metrics.min_value, metrics.max_value, metrics.nulls));
    }
}

std::optional<UInt64> DataLakeColumnStatistics::estimateDistinctValues(
    const Target & target, const DistinctValuesInputs & inputs, const ColumnStats & column, UInt64 rows)
{
    /// An unknown NULL fraction (a file without a NULL count) counts no NULLs, so that the non-NULL rows are not too few.
    /// `value_or` also because the optimizer may call `llround` before checking the optional, and MSan reports the empty
    /// value it reads.
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
    chassert(rows <= total_rows - deleted_rows);
    deleted_rows += std::min(rows, total_rows - deleted_rows);
}

void DataLakeColumnStatistics::finalize(DataLakeReadEstimate & estimate)
{
    const UInt64 live_rows = total_rows - deleted_rows;
    estimate.rows = live_rows;

    /// Selectivities are shares of the live rows. The statistics still count the deleted rows, so their coverage is
    /// checked against all rows of the files: a file without metrics leaves it partial, whatever the deletes.
    statistics_builder.incrementRowCount(total_rows);
    statistics_builder.subtractDeletedRows(deleted_rows);
    const auto estimator = statistics_builder.getEstimator();
    if (!estimator)
        return;

    /// The merged bounds and NULL counts the numbers of distinct values come from. The statistics were built with the
    /// current column types, so there are no outdated ones to skip.
    const auto merged = estimator->estimateRelationProfile(/*metadata=*/nullptr);
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto it = merged.column_stats.find(targets[i].name);
        if (it == merged.column_stats.end())
            continue;
        /// Without one, the estimator keeps its own guess, as for MergeTree without a `uniq` statistic.
        if (const auto distinct_values = estimateDistinctValues(targets[i], distinct_values_inputs[i], it->second, live_rows))
            statistics_builder.setCardinality(targets[i].name, *distinct_values);
    }
    estimate.estimator = estimator;
}

}
