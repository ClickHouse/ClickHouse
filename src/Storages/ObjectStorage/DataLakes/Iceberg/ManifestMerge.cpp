#include "config.h"

#if USE_AVRO

#include <algorithm>

#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestMerge.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>

namespace DB::Setting
{
    extern const SettingsBool iceberg_insert_merge_manifests;
    extern const SettingsBool iceberg_insert_single_manifest_for_all_partitions;
    extern const SettingsUInt64 iceberg_insert_manifest_min_count_to_merge;
    extern const SettingsUInt64 iceberg_insert_manifest_target_size_bytes;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

DataFileColumnStatistics getColumnStatisticsForRewrite(const ParsedManifestFileEntry & entry)
{
    DataFileColumnStatistics stats;
    for (const auto & [field_id, column_info] : entry.columns_infos)
    {
        if (column_info.bytes_size.has_value())
            stats.column_sizes.emplace_back(field_id, *column_info.bytes_size);
        if (column_info.rows_count.has_value())
            stats.value_counts.emplace_back(field_id, *column_info.rows_count);
        if (column_info.nulls_count.has_value())
            stats.null_value_counts.emplace_back(field_id, *column_info.nulls_count);
        if (column_info.nans_count.has_value())
            stats.nan_value_counts.emplace_back(field_id, *column_info.nans_count);
    }
    /// Bounds stay the raw serialized bytes, so they round-trip whatever the column type.
    for (const auto & [field_id, bounds] : entry.value_bounds)
    {
        if (!bounds.first.isNull())
            stats.lower_bounds.emplace_back(field_id, bounds.first.safeGet<String>());
        if (!bounds.second.isNull())
            stats.upper_bounds.emplace_back(field_id, bounds.second.safeGet<String>());
    }
    stats.split_offsets = entry.split_offsets;
    return stats;
}

/// Lineage of an existing manifest entry with the values inherited from its manifest resolved, since an EXISTING entry requires them non-null.
DataFileEntryLineage getEntryLineageForRewrite(const ParsedManifestFileEntry & entry, const ManifestFileCacheKey & manifest)
{
    DataFileEntryLineage lineage;
    lineage.added_snapshot_id = entry.parsed_snapshot_id.value_or(manifest.added_snapshot_id);
    lineage.sequence_number = entry.parsed_sequence_number.value_or(manifest.added_sequence_number);
    /// It can differ from the data sequence number, and when null it inherits the manifest's sequence number too.
    lineage.file_sequence_number = entry.parsed_file_sequence_number.value_or(manifest.added_sequence_number);
    return lineage;
}

bool manifestHasKeyMetadata(const AvroForIcebergDeserializer & deserializer)
{
    if (!deserializer.hasPath(Iceberg::c_data_file_key_metadata))
        return false;
    for (size_t row = 0; row < deserializer.rows(); ++row)
        if (!deserializer.getValueFromRowByName(row, Iceberg::c_data_file_key_metadata).isNull())
            return true;
    return false;
}

std::optional<ManifestMergeSettings> getManifestMergeSettings(const Poco::JSON::Object::Ptr & metadata, const Settings & settings)
{
    if (!settings[Setting::iceberg_insert_merge_manifests] || settings[Setting::iceberg_insert_single_manifest_for_all_partitions])
        return std::nullopt;
    if (metadata->getValue<Int32>(Iceberg::f_format_version) != 2)
        return std::nullopt;

    ManifestMergeSettings result;
    result.min_count_to_merge = settings[Setting::iceberg_insert_manifest_min_count_to_merge];
    result.target_size_bytes = settings[Setting::iceberg_insert_manifest_target_size_bytes];

    if (!metadata->has(Iceberg::f_properties))
        return result;
    auto properties = metadata->getObject(Iceberg::f_properties);
    if (!properties)
        return result;

    if (properties->has("commit.manifest-merge.enabled"))
    {
        const auto enabled = properties->getValue<String>("commit.manifest-merge.enabled");
        if (Poco::icompare(enabled, "false") == 0)
            return std::nullopt;
        if (Poco::icompare(enabled, "true") != 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Iceberg table property 'commit.manifest-merge.enabled' must be 'true' or 'false', got '{}'", enabled);
    }
    if (properties->has("commit.manifest.min-count-to-merge"))
        result.min_count_to_merge = parse<UInt64>(properties->getValue<String>("commit.manifest.min-count-to-merge"));
    if (properties->has("commit.manifest.target-size-bytes"))
        result.target_size_bytes = parse<UInt64>(properties->getValue<String>("commit.manifest.target-size-bytes"));
    return result;
}

namespace
{

std::optional<String> getSinglePartitionKey(const PartitionFieldSummaries & summaries, size_t partition_fields_count)
{
    if (summaries.size() != partition_fields_count)
        return std::nullopt;

    String key;
    for (const auto & field : summaries)
    {
        if (field.contains_nan)
            return std::nullopt;
        if (!field.lower_bound && !field.upper_bound && field.contains_null)
        {
            key += 'N';
        }
        else if (field.lower_bound && field.upper_bound && *field.lower_bound == *field.upper_bound && !field.contains_null)
        {
            key += 'V';
            key += std::to_string(field.lower_bound->size());
            key += ':';
            key += *field.lower_bound;
        }
        else
        {
            return std::nullopt;
        }
    }
    return key;
}

PartitionFieldSummaries toPartitionFieldSummaries(const ManifestPartitionSummary & summary)
{
    PartitionFieldSummaries result;
    for (const auto & field : summary.getFields())
    {
        PartitionFieldSummary & converted = result.emplace_back();
        converted.contains_null = field.contains_null;
        converted.contains_nan = field.contains_nan;
        if (field.bounds_unavailable || !field.lower_bound || !field.upper_bound)
            continue;
        auto lower = dumpFieldToBytes(*field.lower_bound, field.type);
        auto upper = dumpFieldToBytes(*field.upper_bound, field.type);
        converted.lower_bound = String(lower.begin(), lower.end());
        converted.upper_bound = String(upper.begin(), upper.end());
    }
    return result;
}

}

CarriedOverDataFiles collectDataFilesToMerge(
    const ManifestMergeSettings & merge_settings,
    const ManifestFileCacheKeys & parent_manifests,
    const ManifestPartitionSummary * new_manifest_partition_summary,
    size_t partition_fields_count,
    Int32 partition_spec_id,
    Int32 schema_id,
    const String & write_format,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context)
{
    CarriedOverDataFiles result;

    const auto partition_key = getSinglePartitionKey(
        new_manifest_partition_summary ? toPartitionFieldSummaries(*new_manifest_partition_summary) : PartitionFieldSummaries{},
        partition_fields_count);
    if (!partition_key)
        return result;

    std::vector<const ManifestFileCacheKey *> candidates;
    size_t candidates_size = 0;
    for (auto it = parent_manifests.rbegin(); it != parent_manifests.rend(); ++it)
    {
        if (it->content_type != ManifestFileContentType::DATA || it->partition_spec_id != partition_spec_id)
            continue;
        if (getSinglePartitionKey(it->partition_summaries, partition_fields_count) != partition_key)
            continue;
        if (candidates_size + it->manifest_file_byte_size > merge_settings.target_size_bytes)
            break;
        candidates.push_back(&*it);
        candidates_size += it->manifest_file_byte_size;
    }
    if (candidates.empty() || candidates.size() + 1 < merge_settings.min_count_to_merge)
        return result;

    auto log = getLogger("IcebergManifestMerge");
    for (const auto * manifest : candidates)
    {
        auto manifest_file = getManifestFile(object_storage, persistent_table_components, context, log, manifest->manifest_file_path);
        if (manifestHasKeyMetadata(*manifest_file.getDeserializer()))
            continue;

        auto files_handle = getManifestFileEntriesHandle(manifest_file, persistent_table_components, context, *manifest, schema_id);
        const auto & files = files_handle.getFilesWithoutDeleted(FileContentType::DATA);
        if (std::any_of(
                files.begin(),
                files.end(),
                [&](const auto & file)
                { return file->resolved_schema_id != schema_id || Poco::icompare(file->parsed_entry->file_format, write_format) != 0; }))
            continue;

        result.manifest_paths.insert(manifest->manifest_file_path.serialize());
        for (const auto & file : files)
        {
            const auto & entry = *file->parsed_entry;
            auto lineage = getEntryLineageForRewrite(entry, *manifest);
            result.min_sequence_number = std::min(result.min_sequence_number, *lineage.sequence_number);
            result.rows_count += entry.record_count;
            result.paths.push_back(entry.file_path_key);
            result.row_counts.push_back(static_cast<UInt64>(entry.record_count));
            result.byte_counts.push_back(static_cast<UInt64>(entry.file_size_in_bytes));
            result.statistics.push_back(getColumnStatisticsForRewrite(entry));
            result.sort_order_ids.push_back(entry.sort_order_id);
            result.lineage.push_back(std::move(lineage));
        }
    }

    if (!result.manifest_paths.empty())
        LOG_DEBUG(log, "Merging {} manifest files with {} data files into the new manifest", result.manifest_paths.size(), result.paths.size());
    return result;
}

}

#endif
