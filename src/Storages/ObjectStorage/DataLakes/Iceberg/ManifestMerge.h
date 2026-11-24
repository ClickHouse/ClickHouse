#pragma once

#include "config.h"

#include <memory>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
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

struct ManifestEntries
{
    std::vector<IcebergPathFromMetadata> paths;
    std::vector<UInt64> row_counts;
    std::vector<UInt64> byte_counts;
    std::vector<Row> partition_values;
    std::vector<const DataFileStatistics *> fresh_statistics;
    std::vector<DataFileColumnStatistics> carried_over_statistics;
    std::vector<std::optional<Int32>> sort_order_ids;
    std::vector<DataFileEntryLineage> lineage;
};

void appendInsertedFiles(
    ManifestEntries & entries,
    ManifestListEntryCounts & counts,
    const Row & partition_value,
    const MultipleFileWriter & writer,
    std::optional<Int32> sort_order_id);

struct ManifestToMerge
{
    const ManifestFileCacheKey * manifest;
    ManifestFileIterator::ManifestFileEntriesHandle files;
};

std::vector<ManifestToMerge> selectManifestsToMerge(
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

void appendMergedManifests(
    ManifestEntries & entries,
    ManifestListEntryCounts & counts,
    const Row & partition_value,
    const std::vector<ManifestToMerge> & manifests);

}

#endif
