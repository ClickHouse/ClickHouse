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
    extern const int LOGICAL_ERROR;
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

using SinglePartition = std::vector<std::optional<String>>;

std::optional<SinglePartition> getSinglePartition(const PartitionFieldSummaries & summaries)
{
    SinglePartition result;
    for (const auto & field : summaries)
    {
        if (field.contains_nan)
            return std::nullopt;
        if (field.contains_null && !field.lower_bound && !field.upper_bound)
            result.emplace_back();
        else if (!field.contains_null && field.lower_bound && field.lower_bound == field.upper_bound)
            result.emplace_back(*field.lower_bound);
        else
            return std::nullopt;
    }
    return result;
}

std::optional<SinglePartition> getSinglePartition(const Row & partition_value, const DataTypes & partition_types)
{
    ManifestPartitionSummary summary(partition_types);
    summary.add(partition_value);
    SinglePartition result;
    for (const auto & field : summary.getFields())
    {
        if (field.contains_nan || field.bounds_unavailable)
            return std::nullopt;
        if (field.contains_null)
        {
            result.emplace_back();
            continue;
        }
        auto bytes = dumpFieldToBytes(*field.lower_bound, field.type);
        result.emplace_back(String(bytes.begin(), bytes.end()));
    }
    return result;
}

}

SingleInsertDataFiles::SingleInsertDataFiles(Row partition_value_, const MultipleFileWriter & writer_, std::optional<Int32> sort_order_id_)
    : partition_value(std::move(partition_value_))
    , writer(writer_)
    , sort_order_id(sort_order_id_)
{
}

void SingleInsertDataFiles::appendToManifest(ManifestEntries & entries, ManifestListEntryCounts & counts) const
{
    const auto & files = writer.getDataFiles();
    const auto & row_counts = writer.getDataFileRowCounts();
    const auto & byte_counts = writer.getDataFileByteCounts();
    const auto & statistics = writer.getPerFileStatistics();
    if (row_counts.size() != files.size() || byte_counts.size() != files.size() || statistics.size() != files.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Iceberg writer reports {} data files but {} row counts, {} byte counts and {} statistics",
            files.size(),
            row_counts.size(),
            byte_counts.size(),
            statistics.size());

    DataFileEntryLineage lineage;
    lineage.added_by_new_snapshot = true;
    for (size_t i = 0; i < files.size(); ++i)
    {
        entries.paths.push_back(files[i]);
        entries.row_counts.push_back(row_counts[i]);
        entries.byte_counts.push_back(byte_counts[i]);
        entries.partition_values.push_back(partition_value);
        entries.fresh_statistics.push_back(statistics[i].get());
        entries.carried_over_statistics.emplace_back();
        entries.sort_order_ids.push_back(sort_order_id);
        entries.lineage.push_back(lineage);
        counts.files_count += 1;
        counts.rows_count += static_cast<Int64>(row_counts[i]);
    }
}

void MergeWindowDataFiles::appendToManifest(ManifestEntries & entries, ManifestListEntryCounts & counts) const
{
    auto append = [](auto & to, const auto & from) { to.insert(to.end(), from.begin(), from.end()); };
    append(entries.paths, window.paths);
    append(entries.row_counts, window.row_counts);
    append(entries.byte_counts, window.byte_counts);
    append(entries.partition_values, window.partition_values);
    append(entries.fresh_statistics, window.fresh_statistics);
    append(entries.carried_over_statistics, window.carried_over_statistics);
    append(entries.sort_order_ids, window.sort_order_ids);
    append(entries.lineage, window.lineage);
    counts.existing_files_count += static_cast<Int64>(window.paths.size());
    counts.existing_rows_count += rows_count;
    counts.min_sequence_number = std::min(counts.min_sequence_number, min_sequence_number);
}

std::vector<const ManifestFileCacheKey *> selectManifestsToMerge(
    const ManifestMergeSettings & merge_settings,
    const ManifestFileCacheKeys & parent_manifests,
    const Row & partition_value,
    const DataTypes & partition_types,
    Int32 partition_spec_id)
{
    const auto partition = getSinglePartition(partition_value, partition_types);
    if (!partition)
        return {};

    std::vector<const ManifestFileCacheKey *> selected;
    size_t selected_size = 0;
    for (auto it = parent_manifests.rbegin(); it != parent_manifests.rend(); ++it)
    {
        if (it->content_type != ManifestFileContentType::DATA || it->partition_spec_id != partition_spec_id)
            continue;
        if (getSinglePartition(it->partition_summaries) != partition)
            continue;
        if (selected_size + it->manifest_file_byte_size > merge_settings.target_size_bytes)
            break;
        selected.push_back(&*it);
        selected_size += it->manifest_file_byte_size;
    }
    if (selected.size() + 1 < merge_settings.min_count_to_merge)
        return {};
    return selected;
}

MergeWindowDataFiles::MergeWindowDataFiles(
    Row partition_value_,
    Int32 schema_id_,
    String write_format_,
    const PersistentTableComponents & persistent_table_components_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_)
    : partition_value(std::move(partition_value_))
    , schema_id(schema_id_)
    , write_format(std::move(write_format_))
    , persistent_table_components(persistent_table_components_)
    , object_storage(std::move(object_storage_))
    , context(std::move(context_))
{
}

void MergeWindowDataFiles::addManifest(const ManifestFileCacheKey & manifest)
{
    auto log = getLogger("IcebergManifestMerge");
    auto manifest_file = getManifestFile(object_storage, persistent_table_components, context, log, manifest.manifest_file_path);
    if (manifestHasKeyMetadata(*manifest_file.getDeserializer()))
        return;

    auto files_handle = getManifestFileEntriesHandle(manifest_file, persistent_table_components, context, manifest, schema_id);
    const auto & files = files_handle.getFilesWithoutDeleted(FileContentType::DATA);
    if (std::any_of(
            files.begin(),
            files.end(),
            [&](const auto & file)
            { return file->resolved_schema_id != schema_id || Poco::icompare(file->parsed_entry->file_format, write_format) != 0; }))
        return;

    replaced_manifest_paths.insert(manifest.manifest_file_path.serialize());
    for (const auto & file : files)
    {
        const auto & entry = *file->parsed_entry;
        auto lineage = getEntryLineageForRewrite(entry, manifest);
        min_sequence_number = std::min(min_sequence_number, *lineage.sequence_number);
        rows_count += entry.record_count;
        window.paths.push_back(entry.file_path_key);
        window.row_counts.push_back(static_cast<UInt64>(entry.record_count));
        window.byte_counts.push_back(static_cast<UInt64>(entry.file_size_in_bytes));
        window.partition_values.push_back(partition_value);
        window.fresh_statistics.push_back(nullptr);
        window.carried_over_statistics.push_back(getColumnStatisticsForRewrite(entry));
        window.sort_order_ids.push_back(entry.sort_order_id);
        window.lineage.push_back(std::move(lineage));
    }
    LOG_DEBUG(log, "Merging manifest file {} with {} data files into the new manifest", manifest.manifest_file_path.serialize(), files.size());
}

}

#endif
