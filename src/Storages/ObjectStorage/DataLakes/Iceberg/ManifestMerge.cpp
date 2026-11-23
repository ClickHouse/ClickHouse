#include "config.h"

#if USE_AVRO

#include <algorithm>
#include <limits>
#include <map>

#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
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
    if (!settings[Setting::iceberg_insert_merge_manifests])
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

struct MergeCandidate
{
    const ManifestFileCacheKey * parent_manifest = nullptr;
    size_t new_manifest_index = 0;
    Int64 size_bytes = 0;

    bool isNew() const { return parent_manifest == nullptr; }
};

struct MergeGroup
{
    std::vector<MergeCandidate> candidates;
    bool has_new_manifest = false;
};

std::vector<std::vector<MergeCandidate>> packEnd(const std::vector<MergeCandidate> & candidates, size_t target_size_bytes)
{
    std::vector<std::vector<MergeCandidate>> bins;
    std::vector<MergeCandidate> current;
    size_t current_size = 0;
    for (auto it = candidates.rbegin(); it != candidates.rend(); ++it)
    {
        const auto size = static_cast<size_t>(std::max<Int64>(it->size_bytes, 0));
        if (!current.empty() && current_size + size > target_size_bytes)
        {
            bins.push_back(std::move(current));
            current.clear();
            current_size = 0;
        }
        current.push_back(*it);
        current_size += size;
    }
    if (!current.empty())
        bins.push_back(std::move(current));

    std::reverse(bins.begin(), bins.end());
    for (auto & bin : bins)
        std::reverse(bin.begin(), bin.end());
    return bins;
}

}

ManifestMergeResult mergeManifestsOnAppend(
    const ManifestMergeSettings & merge_settings,
    const ManifestMergeTarget & target,
    const ManifestFileCacheKeys & parent_manifests,
    const std::vector<NewManifest> & new_manifests,
    FileNamesGenerator & filename_generator,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    Strings & written_manifests_in_storage)
{
    auto log = getLogger("IcebergManifestMerge");

    ManifestMergeResult result;
    result.is_new_manifest_merged.assign(new_manifests.size(), false);

    const Int64 new_snapshot_id = target.new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
    const Int64 new_sequence_number = target.new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
    const size_t partition_fields_count = target.partition_columns.size();

    auto group_key = [&](const PartitionFieldSummaries & summaries) -> std::optional<String>
    {
        if (target.single_manifest_for_all_partitions)
            return String{};
        return getSinglePartitionKey(summaries, partition_fields_count);
    };

    std::map<String, MergeGroup> groups;
    for (size_t i = 0; i < new_manifests.size(); ++i)
    {
        const auto & new_manifest = new_manifests[i];
        auto key = group_key(
            new_manifest.partition_summary ? toPartitionFieldSummaries(*new_manifest.partition_summary) : PartitionFieldSummaries{});
        if (!key)
            continue;
        auto & group = groups[*key];
        group.candidates.push_back({nullptr, i, new_manifest.size_bytes});
        group.has_new_manifest = true;
    }
    for (auto it = parent_manifests.rbegin(); it != parent_manifests.rend(); ++it)
    {
        if (it->content_type != ManifestFileContentType::DATA || it->partition_spec_id != target.partition_spec_id)
            continue;
        auto key = group_key(it->partition_summaries);
        if (!key)
            continue;
        auto group_it = groups.find(*key);
        if (group_it == groups.end())
            continue;
        group_it->second.candidates.push_back({&*it, 0, static_cast<Int64>(it->manifest_file_byte_size)});
    }

    for (auto & [key, group] : groups)
    {
        if (!group.has_new_manifest)
            continue;

        auto bins = packEnd(group.candidates, merge_settings.target_size_bytes);
        for (size_t bin_index = 0; bin_index < bins.size(); ++bin_index)
        {
            const auto & bin = bins[bin_index];
            if (bin.size() < 2)
                continue;
            if (bin_index == 0 && bin.size() < merge_settings.min_count_to_merge)
                continue;

            std::vector<IcebergPathFromMetadata> file_paths;
            std::vector<Row> file_partition_values;
            std::vector<UInt64> file_row_counts;
            std::vector<UInt64> file_byte_counts;
            std::vector<String> file_formats;
            std::vector<DataFileColumnStatistics> file_statistics;
            std::vector<std::optional<Int32>> file_sort_order_ids;
            std::vector<DataFileEntryLineage> file_entry_lineage;
            std::vector<const MergeCandidate *> merged_candidates;

            MergedManifest merged;
            merged.counts.counts_are_added = true;
            merged.counts.min_sequence_number = std::numeric_limits<Int64>::max();
            if (partition_fields_count > 0)
                merged.partition_summary.emplace(target.partition_types);

            for (const auto & candidate : bin)
            {
                ManifestFileCacheKey manifest_key;
                if (candidate.isNew())
                {
                    const auto & new_manifest = new_manifests[candidate.new_manifest_index];
                    manifest_key = ManifestFileCacheKey{
                        new_manifest.path,
                        static_cast<size_t>(new_manifest.size_bytes),
                        new_sequence_number,
                        new_snapshot_id,
                        ManifestFileContentType::DATA,
                        target.partition_spec_id,
                        {},
                        new_manifest.files_count,
                        std::nullopt};
                }
                else
                {
                    manifest_key = *candidate.parent_manifest;
                }

                auto manifest_file = getManifestFile(object_storage, persistent_table_components, context, log, manifest_key.manifest_file_path);
                if (!candidate.isNew() && manifestHasKeyMetadata(*manifest_file.getDeserializer()))
                    continue;

                auto files_handle
                    = getManifestFileEntriesHandle(manifest_file, persistent_table_components, context, manifest_key, target.schema_id);
                const auto & files = files_handle.getFilesWithoutDeleted(FileContentType::DATA);
                if (!candidate.isNew()
                    && std::any_of(files.begin(), files.end(), [&](const auto & file) { return file->resolved_schema_id != target.schema_id; }))
                    continue;

                merged_candidates.push_back(&candidate);
                for (const auto & file : files)
                {
                    const auto & entry = *file->parsed_entry;
                    file_paths.push_back(entry.file_path_key);
                    file_partition_values.push_back(file->normalized_partition_key_value);
                    file_row_counts.push_back(static_cast<UInt64>(entry.record_count));
                    file_byte_counts.push_back(static_cast<UInt64>(entry.file_size_in_bytes));
                    file_formats.push_back(entry.file_format);
                    file_statistics.push_back(getColumnStatisticsForRewrite(entry));
                    file_sort_order_ids.push_back(entry.sort_order_id);
                    if (merged.partition_summary)
                        merged.partition_summary->add(file->normalized_partition_key_value);

                    if (candidate.isNew())
                    {
                        DataFileEntryLineage lineage;
                        lineage.added_by_new_snapshot = true;
                        file_entry_lineage.push_back(lineage);
                        merged.counts.files_count += 1;
                        merged.counts.rows_count += entry.record_count;
                        merged.counts.min_sequence_number = std::min(merged.counts.min_sequence_number, new_sequence_number);
                    }
                    else
                    {
                        auto lineage = getEntryLineageForRewrite(entry, manifest_key);
                        merged.counts.existing_files_count += 1;
                        merged.counts.existing_rows_count += entry.record_count;
                        merged.counts.min_sequence_number = std::min(merged.counts.min_sequence_number, *lineage.sequence_number);
                        file_entry_lineage.push_back(std::move(lineage));
                    }
                }
            }

            if (merged_candidates.size() < 2)
                continue;

            for (const auto * candidate : merged_candidates)
            {
                if (candidate->isNew())
                    result.is_new_manifest_merged[candidate->new_manifest_index] = true;
                else
                    result.replaced_manifest_paths.insert(candidate->parent_manifest->manifest_file_path.serialize());
            }

            if (file_paths.empty())
                continue;

            merged.path = filename_generator.generateManifestEntryName();
            auto storage_path = persistent_table_components.path_resolver.resolve(merged.path);
            written_manifests_in_storage.push_back(storage_path);

            LOG_DEBUG(log, "Merging {} manifest files with {} data files into {}", merged_candidates.size(), file_paths.size(), storage_path);

            auto buffer = object_storage->writeObject(
                StoredObject(storage_path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
            generateManifestFile(
                target.metadata,
                target.partition_columns,
                file_partition_values,
                target.partition_types,
                file_paths,
                file_row_counts,
                file_byte_counts,
                /*data_file_statistics=*/std::nullopt,
                target.sample_block,
                target.new_snapshot,
                target.write_format,
                target.partition_spec,
                target.partition_spec_id,
                *buffer,
                FileContentType::DATA,
                /*user_defined_sequence_number=*/std::nullopt,
                /*user_defined_snapshot_id=*/std::nullopt,
                file_formats,
                file_statistics,
                file_sort_order_ids,
                file_entry_lineage);
            buffer->finalize();
            merged.size_bytes = static_cast<Int64>(buffer->count());
            if (merged.size_bytes == 0)
                merged.size_bytes = static_cast<Int64>(object_storage->getObjectMetadata(storage_path, /*with_tags=*/false).size_bytes);

            result.merged_manifests.push_back(std::move(merged));
        }
    }

    return result;
}

}

#endif
