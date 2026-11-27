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

}

ManifestColumnStatistics::ManifestColumnStatistics(
    const Names & column_names,
    const ColumnsDescription & columns,
    const IcebergSchemaProcessor & schema_processor_,
    Int32 schema_id,
    ContextPtr context,
    const std::unordered_map<Int32, UInt64> & table_distinct_values)
    : schema_processor(schema_processor_)
    , statistics(std::move(context))
{
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetPhysical(name);
        if (!column)
            continue;

        auto nested_type = removeLowCardinalityAndNullable(column->type);
        if (!DataLakeColumnStatistics::canBeTarget(*nested_type))
            continue;

        /// Represents unchanged column id across all schemas
        auto field_id = schema_processor.tryGetColumnIDByName(schema_id, name);
        if (!field_id)
            continue;

        std::optional<UInt64> distinct_values;
        if (const auto it = table_distinct_values.find(*field_id); it != table_distinct_values.end())
            distinct_values = it->second;
        statistics.addTarget({name, column->type, std::move(nested_type), distinct_values});
        field_ids.push_back(*field_id);
    }
}

const ManifestColumnStatistics::SchemaTypes & ManifestColumnStatistics::getSchemaTypes(Int32 file_schema_id)
{
    auto [it, inserted] = schema_types.try_emplace(file_schema_id);
    if (!inserted)
        /// map already created for this schema, hence return field_id type mappings
        return it->second;

    auto & types_map = it->second;
    for (size_t i = 0; i < field_ids.size(); ++i)
        if (auto field = schema_processor.tryGetFieldCharacteristics(file_schema_id, field_ids[i]))
            types_map.emplace(
                field_ids[i], FieldType{removeLowCardinalityAndNullable(field->type), canStatisticsTrackMinMax(field->type), i});
    return types_map;
}

std::vector<ManifestColumnStatistics::ColumnMetrics> ManifestColumnStatistics::extractFileMetrics(
    const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file, UInt64 rows)
{
    const auto & parsed_entry = *entry.parsed_entry;
    /// Contains all field_id -> type mappings existing for this schema, if field_id did not exist (new column, added recently),
    /// mapping is empty for field_id
    const auto & file_types = getSchemaTypes(entry.resolved_schema_id);

    const auto & targets = statistics.getTargets();
    std::vector<ColumnMetrics> file_metrics(targets.size());
    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & target = targets[i];
        const auto field_id = field_ids[i];
        auto & metrics = file_metrics[i];

        /// A file written before the column was added holds only NULLs in it.
        const auto type_it = file_types.find(field_id);
        if (type_it == file_types.end())
        {
            metrics.nulls = rows;
            continue;
        }
        /// Type with which file was written
        const auto & file_type = type_it->second.nested_type;

        /// Decoded with the type the file was written with, then converted to the current column type: after `int` -> `long`.
        if (type_it->second.tracks_min_max)
        {
            auto bounds = getDataFileColumnBounds(entry, field_id, file_type, path_to_manifest_file);
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

        const auto info_it = parsed_entry.columns_infos.find(field_id);
        const ColumnInfo * info = info_it != parsed_entry.columns_infos.end() ? &info_it->second : nullptr;
        if (info && info->nulls_count && *info->nulls_count >= 0 && static_cast<UInt64>(*info->nulls_count) <= rows)
            metrics.nulls = static_cast<UInt64>(*info->nulls_count);
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
            if (auto value = identityPartitionValue(
                    partition_values[partition_field.tuple_index], file_type, *targets[target_index].nested_type))
                file_metrics[target_index].setIdentityPartitionValue(std::move(*value), rows, tracks_min_max);
        }
    }
    return file_metrics;
}

void ManifestColumnStatistics::addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file)
{
    if (entry.parsed_entry->record_count <= 0)
        return;

    const auto rows = static_cast<UInt64>(entry.parsed_entry->record_count);
    /// Trusted as written, although ClickHouse before 26.5 wrote the row count of the whole commit into each file.
    if (statistics.getTargets().empty())
        statistics.addFile(rows, {});
    else
        statistics.addFile(rows, extractFileMetrics(entry, path_to_manifest_file, rows));
}

void ManifestColumnStatistics::subtractDeletedRows(UInt64 rows)
{
    statistics.subtractDeletedRows(rows);
}

void ManifestColumnStatistics::finalize(DataLakeReadEstimate & estimate)
{
    statistics.finalize(estimate);
}

}

#endif
