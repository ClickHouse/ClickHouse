#pragma once

#include "config.h"

#include <memory>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MultipleFileWriter.h>
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

/// The entries of the data files that `writer` has written into the partition `partition_value`.
std::vector<ManifestFileEntry> getInsertedFileEntries(
    const Row & partition_value, const MultipleFileWriter & writer, std::optional<Int32> sort_order_id);

/// The manifests of the parent snapshot that the new manifest of a partition replaces, and the entries of their files.
struct ManifestsToMerge
{
    std::vector<IcebergPathFromMetadata> manifest_paths;
    std::vector<ManifestFileEntry> entries;
};

/// Selects the newest manifests of the parent snapshot that hold only data files of the partition `partition_value` with
/// the schema and the format of the new files, up to `target_size_bytes` in total. Selects none if, with the new manifest,
/// there would be fewer than `min_count_to_merge`.
ManifestsToMerge selectManifestsToMerge(
    const ManifestMergeSettings & merge_settings,
    const ManifestFileCacheKeys & parent_manifests,
    const Row & partition_value,
    const DataTypes & partition_types,
    Int32 partition_spec_id,
    Int32 schema_id,
    const String & write_format,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context);

}

#endif
