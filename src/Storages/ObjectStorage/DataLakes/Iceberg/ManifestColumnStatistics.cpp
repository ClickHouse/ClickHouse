#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestColumnStatistics.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/convertFieldToType.h>
#include <Poco/String.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/FieldAccurateComparison.h>

#include <algorithm>
#include <limits>

namespace DB::Iceberg
{

namespace
{

/// The raw value of an identity partition field as a value of `storage_type`; Null when it has none.
Field identityPartitionValueToStorageType(const Field & value, const IDataType & storage_type)
{
    const WhichDataType which(storage_type);
    Field result = value;
    /// As in `ManifestFilesPruner::canBePruned`: older ClickHouse writers stored a timestamp as a plain `long`, and a decimal comes as bytes.
    if (value.getType() == Field::Types::Int64 && which.isDateTime64())
        result = DecimalField<Decimal64>(value.safeGet<Int64>(), getDecimalScale(storage_type));
    else if (value.getType() == Field::Types::String && which.isDecimal())
        result = deserializeDecimalFromBinaryRepr(value.safeGet<String>(), storage_type).value_or(Field{});

    /// Only numbers have several raw forms; other values are counted as written.
    if (result.isNull() || !storage_type.isValueRepresentedByNumber())
        return result;
    return convertFieldToType(result, storage_type, nullptr, {}, /*strict=*/ true);
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
    Int32 schema_id)
    : schema_processor(schema_processor_)
{
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetPhysical(name);
        if (!column)
            continue;

        auto nested_type = removeNullable(column->type);
        if (const auto type_id = nested_type->getTypeId();
            type_id == TypeIndex::Tuple || type_id == TypeIndex::Map || type_id == TypeIndex::Array || type_id == TypeIndex::Variant)
            continue;

        auto field_id = schema_processor.tryGetColumnIDByName(schema_id, name);
        if (!field_id)
            continue;

        const bool tracks_min_max = canStatisticsTrackMinMax(column->type);
        targets.push_back(Target{name, *field_id, column->type, std::move(nested_type), tracks_min_max});
    }
    accumulators.resize(targets.size());
}

const ManifestColumnStatistics::FileSchemaColumns & ManifestColumnStatistics::getFileSchemaColumns(Int32 file_schema_id)
{
    auto [it, inserted] = file_schema_columns.try_emplace(file_schema_id);
    if (!inserted)
        return it->second;

    auto & schema_columns = it->second;
    schema_columns.file_types.reserve(targets.size());
    for (const auto & target : targets)
    {
        auto field = schema_processor.tryGetFieldCharacteristics(file_schema_id, target.field_id);
        schema_columns.file_types.push_back(field ? field->type : nullptr);
        if (field && target.tracks_min_max)
            schema_columns.bound_types.emplace(target.field_id, field->type);
    }
    return schema_columns;
}

void ManifestColumnStatistics::addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file)
{
    const auto & parsed_entry = *entry.parsed_entry;
    if (targets.empty() || parsed_entry.record_count <= 0)
        return;

    const auto rows = static_cast<UInt64>(parsed_entry.record_count);
    const auto & schema_columns = getFileSchemaColumns(entry.resolved_schema_id);
    const auto bounds = getDataFileColumnBounds(entry, schema_columns.bound_types, path_to_manifest_file);

    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & target = targets[i];
        auto & accumulator = accumulators[i];

        /// TODO AI made this decision: a surviving file without a metric unsets that statistic of the column; an all-NULL file or a file whose schema predates the column counts as NULLs (issue 120440, plan O5)
        const auto & file_type = schema_columns.file_types[i];
        if (!file_type)
        {
            accumulator.nulls += rows;
            accumulator.identity_partition_known = false;
            continue;
        }

        const auto info_it = parsed_entry.columns_infos.find(target.field_id);
        const ColumnInfo * info = info_it == parsed_entry.columns_infos.end() ? nullptr : &info_it->second;

        bool all_null = false;
        if (info && info->nulls_count && *info->nulls_count >= 0 && static_cast<UInt64>(*info->nulls_count) <= rows)
        {
            accumulator.nulls += static_cast<UInt64>(*info->nulls_count);
            all_null = static_cast<UInt64>(*info->nulls_count) == rows;
        }
        else
            accumulator.nulls_known = false;

        if (info && info->bytes_size && *info->bytes_size >= 0)
            accumulator.sizes += static_cast<UInt64>(*info->bytes_size);
        else
            accumulator.sizes_known = false;

        if (target.tracks_min_max && !accumulator.bounds_missing)
        {
            std::optional<std::pair<Field, Field>> converted;
            if (auto bounds_it = bounds.find(target.field_id); bounds_it != bounds.end())
            {
                /// Decoded with the file's type, so a promoted column (`int` -> `long`) keeps the bounds of its older files.
                const auto file_nested_type = removeNullable(file_type);
                auto lower = convertFieldToType(bounds_it->second.first, *target.nested_type, file_nested_type.get(), {}, /*strict=*/ true);
                auto upper = convertFieldToType(bounds_it->second.second, *target.nested_type, file_nested_type.get(), {}, /*strict=*/ true);
                if (!lower.isNull() && !upper.isNull())
                    converted.emplace(std::move(lower), std::move(upper));
            }

            if (converted)
            {
                if (!accumulator.min_value || accurateLess(converted->first, *accumulator.min_value))
                    accumulator.min_value = std::move(converted->first);
                if (!accumulator.max_value || accurateLess(*accumulator.max_value, converted->second))
                    accumulator.max_value = std::move(converted->second);
            }
            else if (!all_null)
                accumulator.bounds_missing = true;
        }

        if (accumulator.identity_partition_known)
        {
            const PartitionSpecsEntry * identity_field = nullptr;
            if (entry.common_partition_specification)
            {
                for (const auto & partition_field : *entry.common_partition_specification)
                {
                    if (partition_field.source_id == target.field_id && partition_field.tuple_index >= 0
                        && static_cast<size_t>(partition_field.tuple_index) < parsed_entry.partition_key_value.size()
                        && Poco::toLower(partition_field.transform_name) == "identity")
                    {
                        identity_field = &partition_field;
                        break;
                    }
                }
            }

            if (!identity_field)
                accumulator.identity_partition_known = false;
            else if (const auto & value = parsed_entry.partition_key_value[identity_field->tuple_index]; !value.isNull())
            {
                auto converted = identityPartitionValueToStorageType(value, *target.nested_type);
                if (converted.isNull())
                    accumulator.identity_partition_known = false;
                else
                    accumulator.identity_partition_values.insert(std::move(converted));
            }
        }
    }
}

std::unordered_map<String, DataLakeColumnEstimate> ManifestColumnStatistics::finalize(UInt64 rows) const
{
    std::unordered_map<String, DataLakeColumnEstimate> estimates;
    if (rows == 0)
        return estimates;

    for (size_t i = 0; i < targets.size(); ++i)
    {
        const auto & target = targets[i];
        const auto & accumulator = accumulators[i];
        const UInt64 non_null_rows = accumulator.nulls_known ? rows - std::min(accumulator.nulls, rows) : rows;
        const bool has_bounds = !accumulator.bounds_missing && accumulator.min_value && accumulator.max_value;

        DataLakeColumnEstimate estimate;
        using Source = DataLakeColumnEstimate::DistinctValuesSource;
        std::optional<UInt64> range_width;
        if (has_bounds && hasCountableValues(*target.nested_type))
            range_width = valueRangeWidth(*accumulator.min_value, *accumulator.max_value);

        if (non_null_rows == 0)
        {
            estimate.num_distinct_values = 1;
            estimate.distinct_values_source = Source::ColumnType;
        }
        else if (accumulator.identity_partition_known)
        {
            estimate.num_distinct_values = accumulator.identity_partition_values.size();
            estimate.distinct_values_source = Source::IdentityPartition;
        }
        else if (range_width)
        {
            estimate.num_distinct_values = *range_width;
            estimate.distinct_values_source = Source::ValueRange;
        }
        /// TODO AI made this decision: rule 3 divides column_sizes by the fixed width of the type and skips variable-width types (issue 120440, plan O6)
        else if (accumulator.sizes_known && target.nested_type->haveMaximumSizeOfValue())
        {
            estimate.num_distinct_values = accumulator.sizes / target.nested_type->getSizeOfValueInMemory();
            estimate.distinct_values_source = Source::ColumnSize;
        }
        else
        {
            if (isBool(target.nested_type))
                estimate.num_distinct_values = 2;
            else if (isString(target.nested_type))
                estimate.num_distinct_values = rows / 2;
            else
                estimate.num_distinct_values = rows / 10 * 3 + rows % 10 * 3 / 10;
            estimate.distinct_values_source = Source::ColumnType;
        }
        /// A distinct count excludes NULL, as for MergeTree statistics, and 0 would make a join a cross product.
        estimate.num_distinct_values = std::clamp<UInt64>(estimate.num_distinct_values, 1, std::max<UInt64>(non_null_rows, 1));

        if (has_bounds)
        {
            estimate.min_value = accumulator.min_value;
            estimate.max_value = accumulator.max_value;
        }

        if (target.storage_type->isNullable() && accumulator.nulls_known)
            estimate.null_fraction = std::min(1.0, static_cast<Float64>(accumulator.nulls) / static_cast<Float64>(rows));

        estimates.emplace(target.name, std::move(estimate));
    }
    return estimates;
}

}

#endif
