#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestColumnStatistics.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <Poco/String.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/Statistics/Statistics.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace DB::Iceberg
{

namespace
{

/// The decoded value of an identity partition field, written with the type `file_type`, as a value of `storage_type`
/// (see `ColumnMetrics::identity_partition_value`).
std::optional<Field> identityPartitionValue(const Field & value, const DataTypePtr & file_type, const IDataType & storage_type)
{
    if (value.isNull())
        return Field{};

    Field result = convertPartitionValueToType(value, file_type);
    /// A number written with an older type is brought to the storage type, so equal values compare equal.
    if (storage_type.isValueRepresentedByNumber())
        result = convertFieldToType(result, storage_type, file_type.get(), {}, /*strict=*/ true);
    if (result.isNull())
        return std::nullopt;
    return result;
}

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

ManifestColumnStatistics::ManifestColumnStatistics(
    const Names & column_names,
    const ColumnsDescription & columns,
    const IcebergSchemaProcessor & schema_processor_,
    Int32 schema_id,
    ContextPtr context)
    : schema_processor(schema_processor_)
    , statistics_builder(std::move(context))
{
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetPhysical(name);
        if (!column)
            continue;

        auto nested_type = removeLowCardinalityAndNullable(column->type);
        if (const auto type_id = nested_type->getTypeId();
            type_id == TypeIndex::Tuple || type_id == TypeIndex::Map || type_id == TypeIndex::Array || type_id == TypeIndex::Variant)
            continue;

        /// Represents unchanged column id across all schemas
        auto field_id = schema_processor.tryGetColumnIDByName(schema_id, name);
        if (!field_id)
            continue;

        targets.push_back(Target{name, *field_id, column->type, std::move(nested_type)});
    }
    distinct_values_inputs.resize(targets.size());
}

const ManifestColumnStatistics::SchemaTypes & ManifestColumnStatistics::getSchemaTypes(Int32 file_schema_id)
{
    auto [it, inserted] = schema_types.try_emplace(file_schema_id);
    if (!inserted)
        /// map already created for this schema, hence return field_id type mappings
        return it->second;

    auto & types_map = it->second;
    for (size_t i = 0; i < targets.size(); ++i)
        if (auto field = schema_processor.tryGetFieldCharacteristics(file_schema_id, targets[i].field_id))
            types_map.emplace(
                targets[i].field_id, FieldType{removeLowCardinalityAndNullable(field->type), canStatisticsTrackMinMax(field->type), i});
    return types_map;
}

std::vector<ManifestColumnStatistics::ColumnMetrics> ManifestColumnStatistics::extractFileMetrics(
    const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file, UInt64 rows)
{
    const auto & parsed_entry = *entry.parsed_entry;
    /// Contains all field_id -> type mappings existing for this schema, if field_id did not exist (new column, added recently),
    /// mapping is empty for field_id
    const auto & file_types = getSchemaTypes(entry.resolved_schema_id);

    std::vector<ColumnMetrics> file_metrics(targets.size());
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & target = targets[i];
        auto & metrics = file_metrics[i];

        /// A file written before the column was added holds only NULLs in it.
        const auto type_it = file_types.find(target.field_id);
        if (type_it == file_types.end())
        {
            metrics.nulls = rows;
            metrics.size = 0;
            continue;
        }
        /// Type with which file was written
        const auto & file_type = type_it->second.nested_type;

        /// Decoded with the type the file was written with, then converted to the current column type: after `int` -> `long`.
        if (type_it->second.tracks_min_max)
        {
            auto bounds = getDataFileColumnBounds(entry, target.field_id, file_type, path_to_manifest_file);
            if (bounds)
            {
                auto lower = convertFieldToType(bounds->first, *target.nested_type, file_type.get(), {}, /*strict=*/ true);
                auto upper = convertFieldToType(bounds->second, *target.nested_type, file_type.get(), {}, /*strict=*/ true);
                if (!lower.isNull() && !upper.isNull())
                {
                    metrics.min_value = std::move(lower);
                    metrics.max_value = std::move(upper);
                }
            }
        }

        const auto info_it = parsed_entry.columns_infos.find(target.field_id);
        const ColumnInfo * info = info_it != parsed_entry.columns_infos.end() ? &info_it->second : nullptr;
        if (info)
        {
            if (info->nulls_count && *info->nulls_count >= 0 && static_cast<UInt64>(*info->nulls_count) <= rows)
                metrics.nulls = static_cast<UInt64>(*info->nulls_count);
            if (info->bytes_size && *info->bytes_size >= 0)
                metrics.size = static_cast<UInt64>(*info->bytes_size);
        }
    }

    if (entry.common_partition_specification)
    {
        const auto & partition_values = entry.normalized_partition_key_value;
        for (const auto & partition_field : *entry.common_partition_specification)
        {
            if (Poco::icompare(partition_field.transform_name, "identity") != 0 || partition_field.tuple_index < 0
                || static_cast<size_t>(partition_field.tuple_index) >= partition_values.size())
                continue;

            /// `file_types` has only the targets that the file's schema has.
            const auto type_it = file_types.find(partition_field.source_id);
            if (type_it == file_types.end())
                continue;

            const auto & [file_type, tracks_min_max, target_index] = type_it->second;
            auto & metrics = file_metrics[target_index];
            metrics.identity_partition_value = identityPartitionValue(
                partition_values[partition_field.tuple_index], file_type, *targets[target_index].nested_type);
            if (!metrics.identity_partition_value)
                continue;

            /// Every row of the file holds the partition value, so it stands in for a missing NULL count and bounds.
            const auto & value = *metrics.identity_partition_value;
            if (!metrics.nulls)
                metrics.nulls = value.isNull() ? rows : 0;
            if (tracks_min_max && metrics.min_value.isNull() && !value.isNull())
            {
                metrics.min_value = value;
                metrics.max_value = value;
            }
        }
    }
    return file_metrics;
}

void ManifestColumnStatistics::addToDistinctValuesInputs(DistinctValuesInputs & inputs, const ColumnMetrics & metrics)
{
    if (metrics.size)
        inputs.sizes += *metrics.size;
    else
        inputs.sizes_known = false;

    if (!metrics.identity_partition_value)
        inputs.identity_partition_known = false;
    else if (!metrics.identity_partition_value->isNull())
        inputs.identity_partition_values.insert(*metrics.identity_partition_value);
}

void ManifestColumnStatistics::addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file)
{
    if (entry.parsed_entry->record_count <= 0)
        return;

    const auto rows = static_cast<UInt64>(entry.parsed_entry->record_count);
    /// Trusted as written, although ClickHouse before 26.5 wrote the row count of the whole commit into each file.
    total_rows += rows;
    if (targets.empty())
        return;

    const auto file_metrics = extractFileMetrics(entry, path_to_manifest_file, rows);

    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & metrics = file_metrics[i];
        addToDistinctValuesInputs(distinct_values_inputs[i], metrics);
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

std::optional<ManifestColumnStatistics::DistinctValues> ManifestColumnStatistics::estimateDistinctValues(
    const Target & target, const DistinctValuesInputs & inputs, const ColumnStats & column, UInt64 rows)
{
    /// `value_or`: the optimizer may call `llround` before checking the optional, and MSan reports the empty value it reads.
    const UInt64 nulls = static_cast<UInt64>(std::llround(column.null_fraction.value_or(0) * static_cast<Float64>(rows)));
    const UInt64 non_null_rows = rows - std::min(nulls, rows);

    std::optional<UInt64> range_width;
    if (column.min_value && column.max_value && hasCountableValues(*target.nested_type))
        range_width = valueRangeWidth(*column.min_value, *column.max_value);

    UInt64 num_distinct_values = 1;
    bool measured = false;
    if (non_null_rows == 0)
        num_distinct_values = 1;
    else if (inputs.identity_partition_known)
    {
        num_distinct_values = inputs.identity_partition_values.size();
        measured = true;
    }
    /// A timestamp counts microseconds, but its values usually step by thousands of them or more, so its range is far
    /// above the rows and the clamp below gives the estimate.
    else if (range_width)
    {
        num_distinct_values = *range_width;
        /// As MergeTree interpolates ranges between min and max, the values are taken to fill the range, so equality and
        /// `IN` use it; a column with gaps would look more selective than it is
        measured = *range_width <= non_null_rows;
    }
    /// `column_sizes` over the width of a fixed-width type.
    else if (inputs.sizes_known && target.nested_type->haveMaximumSizeOfValue())
        num_distinct_values = inputs.sizes / target.nested_type->getSizeOfValueInMemory();
    else
        return std::nullopt;

    /// A distinct count excludes NULL, as for MergeTree statistics, and 0 would make a join a cross product.
    return DistinctValues{std::clamp<UInt64>(num_distinct_values, 1, std::max<UInt64>(non_null_rows, 1)), measured};
}

void ManifestColumnStatistics::finalize(DataLakeReadEstimate & estimate)
{
    estimate.rows = total_rows;

    /// Selectivities are shares of these rows.
    statistics_builder.incrementRowCount(total_rows);
    const auto estimator = statistics_builder.getEstimator();
    if (!estimator)
        return;

    /// The merged bounds and NULL counts the numbers of distinct values come from.
    const auto merged = estimator->estimateRelationProfile();
    /// Next step estimates NDV count when possible, from data we have from min/max, primary key info, etc.
    /// Similar to MergeTree without `uniq` statistics, estimator would make a blind guess.
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & col_target = targets[i];
        const auto & col_name = col_target.name;
        const auto it = merged.column_stats.find(col_name);
        if (it == merged.column_stats.end())
            continue;
        const auto & column_stats = it->second;
        const auto & distinct_value_input = distinct_values_inputs[i];
        /// Without one, the estimator keeps its own guess, as for MergeTree without a `uniq` statistic.
        if (const auto distinct_values = estimateDistinctValues(col_target, distinct_value_input, column_stats, total_rows))
            statistics_builder.setCardinality(col_name, distinct_values->count, distinct_values->measured);
    }
    estimate.estimator = estimator;
}

}

#endif
