#pragma once

#include "config.h"

#include <limits>
#include <unordered_set>

#if USE_AVRO

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

struct CarriedOverDataFiles
{
    std::unordered_set<String> manifest_paths;
    std::vector<IcebergPathFromMetadata> paths;
    std::vector<UInt64> row_counts;
    std::vector<UInt64> byte_counts;
    std::vector<DataFileColumnStatistics> statistics;
    std::vector<std::optional<Int32>> sort_order_ids;
    std::vector<DataFileEntryLineage> lineage;
    Int64 rows_count = 0;
    Int64 min_sequence_number = std::numeric_limits<Int64>::max();
};

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
    ContextPtr context);

}

#endif
