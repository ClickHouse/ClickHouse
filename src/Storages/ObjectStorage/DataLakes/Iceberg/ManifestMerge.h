#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

namespace DB
{
struct Settings;
}

namespace DB::Iceberg
{

class AvroForIcebergDeserializer;

DataFileColumnStatistics getColumnStatisticsForRewrite(const ParsedManifestFileEntry & entry);

DataFileEntryLineage getEntryLineageForRewrite(const ParsedManifestFileEntry & entry, const ManifestFileCacheKey & manifest);

bool manifestHasKeyMetadata(const AvroForIcebergDeserializer & deserializer);

struct ManifestMergeSettings
{
    size_t min_count_to_merge = 0;
    size_t target_size_bytes = 0;
};

std::optional<ManifestMergeSettings> getManifestMergeSettings(const Poco::JSON::Object::Ptr & metadata, const Settings & settings);

struct NewManifest
{
    IcebergPathFromMetadata path;
    Int64 size_bytes = 0;
    Int64 files_count = 0;
    const ManifestPartitionSummary * partition_summary = nullptr;
};

struct MergedManifest
{
    IcebergPathFromMetadata path;
    Int64 size_bytes = 0;
    ManifestListEntryCounts counts;
    std::optional<ManifestPartitionSummary> partition_summary;
};

struct ManifestMergeResult
{
    std::vector<MergedManifest> merged_manifests;
    std::vector<bool> is_new_manifest_merged;
    std::unordered_set<String> replaced_manifest_paths;

    bool empty() const { return replaced_manifest_paths.empty() && merged_manifests.empty(); }
};

struct ManifestMergeTarget
{
    Poco::JSON::Object::Ptr metadata;
    Poco::JSON::Object::Ptr new_snapshot;
    Poco::JSON::Object::Ptr partition_spec;
    Int32 partition_spec_id = 0;
    std::vector<String> partition_columns;
    DataTypes partition_types;
    Int32 schema_id = 0;
    SharedHeader sample_block;
    String write_format;
    bool single_manifest_for_all_partitions = false;
};

ManifestMergeResult mergeManifestsOnAppend(
    const ManifestMergeSettings & merge_settings,
    const ManifestMergeTarget & target,
    const ManifestFileCacheKeys & parent_manifests,
    const std::vector<NewManifest> & new_manifests,
    FileNamesGenerator & filename_generator,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    Strings & written_manifests_in_storage);

}

#endif
