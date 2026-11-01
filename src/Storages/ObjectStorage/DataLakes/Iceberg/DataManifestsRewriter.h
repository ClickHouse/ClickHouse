#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Block_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#include <map>
#include <span>
#include <unordered_set>

#include <Poco/JSON/Object.h>

namespace DB::Iceberg
{

/// Rewrites the live entries of a set of data manifests into new data manifests, without touching the data files.
/// Each data file is written as an EXISTING entry that keeps its lineage (snapshot id and sequence numbers), format,
/// statistics, sort order and partition tuple, so the rewrite is invisible to readers except for the manifest count.
/// Used by `OPTIMIZE TABLE ... MANIFEST` and by the merge of manifests when appending to a table.
class DataManifestsRewriter
{
public:
    enum class Grouping : uint8_t
    {
        /// One output manifest per (partition spec, schema, partition tuple).
        PerPartition,
        /// Output manifests span partitions: files are grouped only by (partition spec, schema), ordered by their
        /// partition tuple so each manifest covers a narrow range of partitions, and split into manifests of bounded size.
        SpanPartitions,
    };

    DataManifestsRewriter(
        Poco::JSON::Object::Ptr metadata_object_,
        const PersistentTableComponents & persistent_table_components_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        SharedHeader sample_block_,
        String write_format_,
        Grouping grouping_);

    /// Reads the data manifests and collects their live data files. Can be called once.
    void collect(const ManifestFileCacheKeys & data_manifests);

    size_t getDataFilesCount() const { return data_files_count; }

    /// Whether a collected data file has `key_metadata` (an encrypted data file). A rewrite cannot carry it over,
    /// so the caller must not call `write` then.
    bool hasEncryptedDataFiles() const { return has_encrypted_data_files; }

    /// Whether a collected data file has a partition value that `generateManifestFile` cannot write.
    bool hasUnsupportedPartitionValues() const { return has_unsupported_partition_values; }

    /// The number of manifests `write` produces. `max_entries_per_manifest` is used only with `Grouping::SpanPartitions`.
    size_t getOutputManifestsCount(size_t max_entries_per_manifest) const;

    /// The new manifests, parallel arrays in the form `generateManifestList` takes them.
    struct Result
    {
        std::vector<IcebergPathFromMetadata> paths;
        std::vector<Int64> sizes;
        std::vector<ManifestListEntryCounts> counts;
        std::vector<Int64> partition_spec_ids;
        std::vector<ManifestPartitionSummary> partition_summaries;
    };

    /// Writes the manifests for the snapshot `new_snapshot`. Each path is appended to `result.paths` before its object
    /// is created, so after an exception the caller can remove every object that may have been written.
    void write(FileNamesGenerator & generator, Poco::JSON::Object::Ptr new_snapshot, size_t max_entries_per_manifest, Result & result);

private:
    struct DataFile
    {
        IcebergPathFromMetadata path;
        Row partition_values;
        Int64 record_count = 0;
        Int64 file_size_in_bytes = 0;
        String file_format;
        DataFileColumnStatistics statistics;
        std::optional<Int32> sort_order_id;
        DataFileEntryLineage lineage;
    };

    struct Group
    {
        /// The partition spec the source files were written with; the rewritten manifest reuses it.
        Int32 partition_spec_id = 0;
        /// The schema the source files were written under; files of different schemas are grouped separately so each
        /// rewritten manifest's `schema` header matches all its entries.
        Int32 schema_id = 0;
        std::vector<DataFile> files;
        std::unordered_set<String> file_paths;
    };

    struct ResolvedPartitionSpec
    {
        Poco::JSON::Object::Ptr spec;
        std::vector<String> partition_columns;
        DataTypes partition_types;
    };

    const ResolvedPartitionSpec & resolvePartitionSpec(Int32 spec_id, Int32 files_schema_id);
    Poco::JSON::Object::Ptr getSchemaObjectById(Int32 schema_id) const;

    void writeManifest(
        FileNamesGenerator & generator,
        Poco::JSON::Object::Ptr new_snapshot,
        const Group & group,
        std::span<const DataFile> files,
        Result & result);

    Poco::JSON::Object::Ptr metadata_object;
    const PersistentTableComponents & persistent_table_components;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    SharedHeader sample_block;
    String write_format;
    Grouping grouping;
    LoggerPtr log;

    Int32 current_schema_id = 0;
    Poco::JSON::Array::Ptr schemas;
    Poco::JSON::Array::Ptr partition_specs;

    /// Ordered, so the output does not depend on hashing.
    std::map<String, Group> groups;
    std::map<std::pair<Int32, Int32>, ResolvedPartitionSpec> resolved_specs;
    size_t data_files_count = 0;
    bool has_encrypted_data_files = false;
    bool has_unsupported_partition_values = false;
    bool collected = false;
};

}

#endif
