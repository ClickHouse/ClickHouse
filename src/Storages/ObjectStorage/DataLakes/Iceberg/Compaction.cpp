#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <Databases/DataLake/Common.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MultipleFileWriter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FieldVisitorDump.h>
#include <Common/Logger.h>

#if USE_AVRO && !CLICKHOUSE_CLOUD

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
    extern const int UNSUPPORTED_METHOD;
}

namespace DB::FailPoints
{
    extern const char iceberg_compaction_pause_before_metadata_commit[];
}

namespace DB::Setting
{
    extern const SettingsUInt64 iceberg_manifest_min_count_to_compact;
}

namespace DB::DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace DB::Iceberg
{

static constexpr size_t MAX_COMPACTION_RETRIES = 100;

using namespace DB;

/// Cheap pre-check for `compactIcebergManifests`: read just the current manifest list and report whether its entry count exceeds `threshold`.
static bool isCurrentManifestListAboveThreshold(
    Poco::JSON::Object::Ptr metadata_object,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    size_t threshold)
{
    LoggerPtr log = getLogger("IcebergCompaction::isCurrentManifestListAboveThreshold");

    if (!metadata_object->has(Iceberg::f_current_snapshot_id))
        return false;
    Int64 current_snapshot_id = metadata_object->getValue<Int64>(Iceberg::f_current_snapshot_id);
    if (current_snapshot_id < 0)
        return false;

    String current_manifest_list_path;
    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == current_snapshot_id)
        {
            current_manifest_list_path = snapshot->getValue<String>(Iceberg::f_manifest_list);
            break;
        }
    }
    if (current_manifest_list_path.empty())
        return false;

    auto filename = IcebergPathFromMetadata::deserialize(current_manifest_list_path);
    RelativePathWithMetadata object_info(persistent_table_components.path_resolver.resolve(filename));
    auto manifest_list_buf = createReadBuffer(object_info, object_storage, context, log);
    AvroForIcebergDeserializer manifest_list_deserializer(
        std::move(manifest_list_buf), filename, getFormatSettings(context));
    return manifest_list_deserializer.rows() > threshold;
}

namespace
{

struct ResolvedPartitionSpec
{
    Poco::JSON::Object::Ptr spec;
    std::vector<String> partition_columns;
    DataTypes partition_types;
};

/// After partition evolution each manifest must be rewritten under the spec its source files used, and after schema evolution
/// (e.g. widening `decimal(P, S)`) its partition values must be encoded under the schema its source files were written with;
/// resolve and cache spec info per (spec-id, schema-id).
class PartitionSpecResolver
{
public:
    PartitionSpecResolver(Poco::JSON::Object::Ptr metadata_object, const PersistentTableComponents & persistent_table_components_, ContextPtr context_)
        : schemas(metadata_object->getArray(Iceberg::f_schemas))
        , partitions_specs(metadata_object->getArray(f_partition_specs))
        , current_schema_id(metadata_object->getValue<Int32>(Iceberg::f_current_schema_id))
        , persistent_table_components(persistent_table_components_)
        , context(context_)
    {
    }

    const ResolvedPartitionSpec & resolve(Int32 spec_id, Int32 files_schema_id)
    {
        if (auto it = resolved_specs.find({spec_id, files_schema_id}); it != resolved_specs.end())
            return it->second;

        Poco::JSON::Object::Ptr spec;
        for (UInt32 i = 0; i < partitions_specs->size(); ++i)
        {
            auto candidate = partitions_specs->getObject(i);
            if (candidate->getValue<Int64>(Iceberg::f_spec_id) == spec_id)
            {
                spec = candidate;
                break;
            }
        }
        if (!spec)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Iceberg metadata does not contain a partition spec entry matching spec-id {}",
                spec_id);

        ResolvedPartitionSpec resolved;
        resolved.spec = spec;
        auto spec_fields = spec->getArray(f_fields);

        /// Partition field names and the schema source-ids they transform.
        std::vector<Int32> source_ids;
        for (UInt32 i = 0; i < spec_fields->size(); ++i)
        {
            auto spec_field = spec_fields->getObject(i);
            resolved.partition_columns.push_back(spec_field->getValue<String>(f_name));
            source_ids.push_back(spec_field->getValue<Int32>(Iceberg::f_source_id));
        }

        /// Derive partition value types from a schema that defines every source column the spec references, preferring the schema
        /// the source files were written with, then the current one, then any historical one; register all schemas first so they can be queried by id.
        for (UInt32 i = 0; i < schemas->size(); ++i)
            persistent_table_components.schema_processor->addIcebergTableSchema(schemas->getObject(i));

        auto build_sample_block = [&](Int32 schema_id) -> std::optional<Block>
        {
            auto fields_characteristics
                = persistent_table_components.schema_processor->tryGetFieldsCharacteristics(schema_id, source_ids);
            /// A short result means this schema does not define every partition source column.
            if (fields_characteristics.size() != source_ids.size())
                return std::nullopt;
            Block block;
            for (const auto & name_and_type : fields_characteristics)
                block.insert(ColumnWithTypeAndName(name_and_type.type, name_and_type.name));
            return block;
        };

        Int32 schema_id_for_spec = files_schema_id;
        std::optional<Block> spec_sample_block = build_sample_block(schema_id_for_spec);
        if (!spec_sample_block && files_schema_id != current_schema_id)
        {
            schema_id_for_spec = current_schema_id;
            spec_sample_block = build_sample_block(schema_id_for_spec);
        }
        if (!spec_sample_block)
        {
            for (UInt32 i = 0; i < schemas->size(); ++i)
            {
                Int32 candidate_id = schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id);
                if (candidate_id == files_schema_id || candidate_id == current_schema_id)
                    continue;
                spec_sample_block = build_sample_block(candidate_id);
                if (spec_sample_block)
                {
                    schema_id_for_spec = candidate_id;
                    break;
                }
            }
        }
        if (!spec_sample_block)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "No Iceberg schema defines all source columns referenced by partition spec {}",
                spec_id);

        auto schema_for_spec = persistent_table_components.schema_processor->getIcebergTableSchemaById(schema_id_for_spec);
        auto shared_sample_block = std::make_shared<const Block>(std::move(*spec_sample_block));
        resolved.partition_types
            = ChunkPartitioner(spec_fields, schema_for_spec->getArray(Iceberg::f_fields), context, shared_sample_block).getResultTypes();

        return resolved_specs.emplace(std::make_pair(spec_id, files_schema_id), std::move(resolved)).first->second;
    }

    /// Return the raw metadata schema object for a given schema-id, used as the verbatim Avro `schema` header of a rewritten manifest so its data-file bounds resolve under the same schema the files were written with.
    Poco::JSON::Object::Ptr getSchemaObject(Int32 schema_id) const
    {
        for (UInt32 i = 0; i < schemas->size(); ++i)
            if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == schema_id)
                return schemas->getObject(i);
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg metadata does not contain a schema entry matching schema-id {}",
            schema_id);
    }

private:
    Poco::JSON::Array::Ptr schemas;
    Poco::JSON::Array::Ptr partitions_specs;
    Int32 current_schema_id;
    const PersistentTableComponents & persistent_table_components;
    ContextPtr context;
    std::map<std::pair<Int32, Int32>, ResolvedPartitionSpec> resolved_specs;
};

/// Data files written into one manifest: a manifest carries a single partition spec and one `schema` header.
struct ManifestFilesGroup
{
    Int32 partition_spec_id = 0;
    Int32 schema_id = 0;
    Row partition_values;
    Iceberg::FileContentType content_type = Iceberg::FileContentType::DATA;
    std::vector<IcebergPathFromMetadata> file_paths;
    /// Parallel to file_paths: {record_count, file_size_in_bytes}.
    std::vector<std::pair<Int64, Int64>> file_metrics;
    /// Parallel to file_paths: the original file_format, preserved so a rewrite never relabels the file's format.
    std::vector<String> file_formats;
    /// Parallel to file_paths: the source file's sort_order_id, preserved so the rewrite keeps sortedness.
    std::vector<std::optional<Int32>> file_sort_order_ids;
    /// Parallel to file_paths for EXISTING entries: the source entry's lineage and per-column statistics, carried over verbatim.
    std::vector<DataFileEntryLineage> file_entry_lineage;
    std::vector<DataFileColumnStatistics> file_statistics;
    /// Parallel to file_paths for ADDED entries: the statistics of each newly written file.
    std::vector<const DataFileStatistics *> file_fresh_statistics;
};

/// Keyed by spec-id, schema-id and partition value; FieldVisitorDump's type tag prevents UInt64/Int64 collisions.
using ManifestFilesGroups = std::unordered_map<String, ManifestFilesGroup>;

ManifestFilesGroup & getManifestFilesGroup(ManifestFilesGroups & groups, Int32 partition_spec_id, Int32 schema_id, const Row & partition_values)
{
    String group_key = std::to_string(partition_spec_id) + "|" + std::to_string(schema_id) + "|";
    FieldVisitorDump dump_visitor;
    for (const auto & val : partition_values)
        group_key += applyVisitor(dump_visitor, val) + "|";

    auto & group = groups[group_key];
    group.partition_spec_id = partition_spec_id;
    group.schema_id = schema_id;
    group.partition_values = partition_values;
    return group;
}

/// Adds a live file as an EXISTING entry, or as one DELETED by the new snapshot, that keeps its lineage and statistics.
/// Returns false if its group already lists it.
bool addExistingDataFile(
    ManifestFilesGroups & groups, const ManifestFileCacheKey & manifest_file, const ProcessedManifestFileEntry & data_file, bool deleted = false)
{
    auto & group = getManifestFilesGroup(groups, manifest_file.partition_spec_id, data_file.resolved_schema_id, data_file.normalized_partition_key_value);

    // A single manifest file should not list the same data file twice
    if (std::find(group.file_paths.begin(), group.file_paths.end(), data_file.parsed_entry->file_path_key) != group.file_paths.end())
        return false;

    group.file_paths.push_back(data_file.parsed_entry->file_path_key);
    group.file_metrics.emplace_back(data_file.parsed_entry->record_count, data_file.parsed_entry->file_size_in_bytes);
    group.file_formats.push_back(data_file.parsed_entry->file_format);
    group.file_sort_order_ids.push_back(data_file.parsed_entry->sort_order_id);

    /// Preserve the entry's lineage, resolving inherited (null) snapshot-id and sequence numbers from the manifest, since EXISTING entries require them non-null.
    DataFileEntryLineage lineage;
    lineage.added_snapshot_id = data_file.parsed_entry->parsed_snapshot_id;
    if (!lineage.added_snapshot_id.has_value())
        lineage.added_snapshot_id = manifest_file.added_snapshot_id;
    lineage.sequence_number = data_file.parsed_entry->parsed_sequence_number;
    if (!lineage.sequence_number.has_value())
        lineage.sequence_number = manifest_file.added_sequence_number;
    /// `file_sequence_number` is preserved separately: it can differ from the data sequence number and, when null, inherits the manifest's sequence number.
    lineage.file_sequence_number = data_file.parsed_entry->parsed_file_sequence_number;
    if (!lineage.file_sequence_number.has_value())
        lineage.file_sequence_number = manifest_file.added_sequence_number;
    lineage.deleted = deleted;
    group.file_entry_lineage.push_back(lineage);

    /// Carry the source file's per-column stats over verbatim, keeping bounds as the raw serialized bytes so they round-trip.
    DataFileColumnStatistics stats;
    for (const auto & [field_id, col_info] : data_file.parsed_entry->columns_infos)
    {
        if (col_info.bytes_size.has_value())
            stats.column_sizes.emplace_back(field_id, *col_info.bytes_size);
        if (col_info.rows_count.has_value())
            stats.value_counts.emplace_back(field_id, *col_info.rows_count);
        if (col_info.nulls_count.has_value())
            stats.null_value_counts.emplace_back(field_id, *col_info.nulls_count);
    }
    for (const auto & [field_id, bounds] : data_file.parsed_entry->value_bounds)
    {
        if (!bounds.first.isNull())
            stats.lower_bounds.emplace_back(field_id, bounds.first.safeGet<String>());
        if (!bounds.second.isNull())
            stats.upper_bounds.emplace_back(field_id, bounds.second.safeGet<String>());
    }
    group.file_statistics.push_back(std::move(stats));
    return true;
}

struct WrittenManifest
{
    IcebergPathFromMetadata path;
    Int64 size = 0;
    ManifestListEntryCounts counts;
    Int64 partition_spec_id = 0;
    /// The manifest's partition fields (value + type), used to write the manifest-list `partitions` summary.
    std::vector<std::pair<Field, DataTypePtr>> partition_summary;
};

/// Writes `group` as one manifest of `new_snapshot`. The path is appended to `written_paths` before the object is created, so
/// the caller can remove it even if a later step throws.
WrittenManifest writeManifestFilesGroup(
    const String & group_key,
    const ManifestFilesGroup & group,
    PartitionSpecResolver & spec_resolver,
    Poco::JSON::Object::Ptr metadata_object,
    Poco::JSON::Object::Ptr new_snapshot,
    FileNamesGenerator & generator,
    SharedHeader sample_block,
    const String & write_format,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    std::vector<IcebergPathFromMetadata> & written_paths,
    LoggerPtr log)
{
    WrittenManifest result;
    result.path = generator.generateManifestEntryName();
    auto storage_manifest_path = path_resolver.resolve(result.path);
    LOG_INFO(log, "Creating manifest file for partition '{}': {} ({} data files)",
             group_key, storage_manifest_path, group.file_paths.size());

    written_paths.push_back(result.path);

    auto buffer_manifest = object_storage->writeObject(
        StoredObject(storage_manifest_path),
        WriteMode::Rewrite,
        std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        context->getWriteSettings());

    /// EXISTING files keep their original sequence numbers, so min_sequence_number is the minimum over the live ones; ADDED files get the
    /// new snapshot's, and so does a manifest without live entries.
    const bool entries_are_added = group.file_entry_lineage.empty();
    Int64 manifest_min_sequence_number = std::numeric_limits<Int64>::max();
    std::vector<UInt64> file_row_counts;
    std::vector<UInt64> file_byte_counts;
    file_row_counts.reserve(group.file_metrics.size());
    file_byte_counts.reserve(group.file_metrics.size());
    for (size_t i = 0; i < group.file_metrics.size(); ++i)
    {
        const auto [record_count, file_size_in_bytes] = group.file_metrics[i];
        file_row_counts.push_back(static_cast<UInt64>(record_count));
        file_byte_counts.push_back(static_cast<UInt64>(file_size_in_bytes));
        if (!entries_are_added && group.file_entry_lineage[i].deleted)
        {
            result.counts.deleted_files_count += 1;
            result.counts.deleted_rows_count += record_count;
            continue;
        }
        result.counts.files_count += 1;
        result.counts.rows_count += record_count;
        if (!entries_are_added)
            manifest_min_sequence_number = std::min(manifest_min_sequence_number, group.file_entry_lineage[i].sequence_number.value_or(0));
    }
    if (entries_are_added || result.counts.files_count == 0)
        manifest_min_sequence_number = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);

    result.counts.min_sequence_number = manifest_min_sequence_number;
    result.counts.counts_are_added = entries_are_added;

    /// Rewrite this manifest under the partition spec and the schema its files use, not the default and the current ones.
    const auto & resolved_spec = spec_resolver.resolve(group.partition_spec_id, group.schema_id);
    result.partition_spec_id = group.partition_spec_id;

    /// The manifest's partition tuple must match the resolved spec; a mismatch (corrupt or inconsistently-evolved
    /// metadata) would otherwise read past the end of partition_values while writing the manifest.
    if (group.partition_values.size() != resolved_spec.partition_columns.size())
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg manifest partition tuple has {} values but partition spec {} defines {} columns",
            group.partition_values.size(),
            group.partition_spec_id,
            resolved_spec.partition_columns.size());

    /// All files in this manifest share one partition value, so the summary's lower/upper bounds are exactly that value.
    result.partition_summary.reserve(resolved_spec.partition_types.size());
    for (size_t i = 0; i < resolved_spec.partition_types.size(); ++i)
        result.partition_summary.emplace_back(group.partition_values[i], resolved_spec.partition_types[i]);

    generateManifestFile(
        metadata_object,
        resolved_spec.partition_columns,
        group.partition_values,
        resolved_spec.partition_types,
        group.file_paths,
        file_row_counts,
        file_byte_counts,
        std::nullopt,
        sample_block,
        new_snapshot,
        write_format,
        resolved_spec.spec,
        group.partition_spec_id,
        *buffer_manifest,
        group.content_type,
        /* user_defined_sequence_number */ std::nullopt,
        /* user_defined_snapshot_id */ std::nullopt,
        /* data_file_formats */ group.file_formats,
        /* per_file_statistics */ group.file_statistics,
        /* data_file_sort_order_ids */ group.file_sort_order_ids,
        /* per_file_entry_lineage */ group.file_entry_lineage,
        /* schema_to_serialize */ spec_resolver.getSchemaObject(group.schema_id),
        /* per_file_fresh_statistics */ entries_are_added ? &group.file_fresh_statistics : nullptr);

    buffer_manifest->finalize();
    result.size = buffer_manifest->count();
    if (result.size == 0)
        result.size = object_storage->getObjectMetadata(storage_manifest_path, /*with_tags=*/false).size_bytes;
    return result;
}

/// A manifest rewrite cannot round-trip per-file `key_metadata` (data-file encryption keys), so reject rather than silently
/// dropping it and making an encrypted table unreadable.
void checkManifestsHaveNoKeyMetadata(
    const std::vector<IcebergPathFromMetadata> & manifest_paths,
    std::string_view operation,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    LoggerPtr log)
{
    for (const auto & manifest_path : manifest_paths)
    {
        RelativePathWithMetadata key_metadata_object_info(path_resolver.resolve(manifest_path));
        auto key_metadata_buf = createReadBuffer(key_metadata_object_info, object_storage, context, log);
        AvroForIcebergDeserializer key_metadata_deserializer(std::move(key_metadata_buf), manifest_path, getFormatSettings(context));
        if (!key_metadata_deserializer.hasPath(c_data_file_key_metadata))
            continue;
        for (size_t row = 0; row < key_metadata_deserializer.rows(); ++row)
            if (!key_metadata_deserializer.getValueFromRowByName(row, c_data_file_key_metadata).isNull())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "{} is not supported for Iceberg tables with per-file key_metadata "
                    "(encrypted data files): preserving the encryption metadata across a manifest rewrite is not implemented",
                    operation);
    }
}

enum class CommitOutcome : uint8_t
{
    Committed,
    /// Provably not published: the attempt's files can be removed and the operation retried.
    Conflict,
    /// The commit may or may not have been applied.
    Unknown,
};

/// The metadata file the catalog currently points at, resolved as at the start of a compaction attempt.
MetadataFileWithInfo getCatalogCurrentMetadataFile(
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const DataLakeStorageSettings & data_lake_settings,
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const StorageID & table_id,
    LoggerPtr log)
{
    return getLatestMetadataFileAndVersionWithCatalog(
        object_storage,
        catalog,
        table_id.getTableName(),
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context,
        log.get(),
        persistent_table_components.table_uuid,
        persistent_table_components.metadata_compression_method,
        /* ignore_metadata_pointer_overrides */ true);
}

/// Publishes `metadata_object` with `new_snapshot` as the current snapshot: through the catalog when one owns the table,
/// else as the next metadata file + version hint. Exceptions mean the outcome is unknown.
CommitOutcome commitCompactedMetadata(
    Poco::JSON::Object::Ptr metadata_object,
    const GeneratedMetadataFileWithInfo & metadata_info,
    FileNamesGenerator & generator,
    Poco::JSON::Object::Ptr new_snapshot,
    const IcebergPathResolver & path_resolver,
    const PersistentTableComponents & persistent_table_components,
    const String & planned_metadata_file_path,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const DataLakeStorageSettings & data_lake_settings,
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const StorageID & table_id,
    LoggerPtr log)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(metadata_object, oss, 4);
    std::string json_representation = removeEscapedSlashes(oss.str());

    auto hint_path = generator.generateVersionHint();
    LOG_INFO(log, "Committing metadata file: {}", path_resolver.resolve(metadata_info.path));

    /// A transactional catalog is the source of truth for the current metadata; for it the storage-side
    /// version hint is irrelevant, so only write the metadata file + version hint when no such catalog owns the table.
    const bool catalog_writes_metadata_file = catalog && catalog->isTransactional();
    if (!catalog_writes_metadata_file)
    {
        /// A non-transactional catalog swaps its pointer unconditionally, so a commit made since planning is checked here.
        if (catalog)
        {
            const auto current_metadata_file = getCatalogCurrentMetadataFile(
                persistent_table_components, object_storage, context, data_lake_settings, catalog, table_id, log);
            if (current_metadata_file.path != planned_metadata_file_path)
            {
                LOG_INFO(log, "The catalog moved past the planned metadata, retrying");
                return CommitOutcome::Conflict;
            }
        }

        switch (writeMetadataFileAndVersionHintWithOutcome(
            path_resolver,
            metadata_info,
            json_representation,
            hint_path,
            object_storage,
            context,
            data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
        {
            case MetadataWriteOutcome::Written:
                break;
            case MetadataWriteOutcome::TargetExisted:
                LOG_INFO(log, "Metadata commit conflict detected");
                return CommitOutcome::Conflict;
            case MetadataWriteOutcome::WriteFailed:
                return CommitOutcome::Unknown;
        }
    }

    /// Advance the catalog pointer to the new metadata so catalog-based readers see the compacted snapshot.
    if (catalog)
    {
        auto catalog_filename = path_resolver.resolveForCatalog(metadata_info.path);
        const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
        if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
        {
            if (!catalog_writes_metadata_file)
                return CommitOutcome::Unknown;

            /// The HTTP client re-sends the commit, so a rejection is checked against the catalog's snapshots.
            try
            {
                const auto current_metadata_file = getCatalogCurrentMetadataFile(
                    persistent_table_components, object_storage, context, data_lake_settings, catalog, table_id, log);
                auto current_metadata = getMetadataJSONObject(
                    current_metadata_file.path,
                    object_storage,
                    persistent_table_components.metadata_cache,
                    context,
                    log,
                    current_metadata_file.compression_method,
                    persistent_table_components.table_uuid);

                const auto our_id = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
                const auto parent_id = new_snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
                const auto our_sequence_number = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
                struct RetainedSnapshot
                {
                    std::optional<Int64> parent;
                    Int64 sequence_number = 0;
                };
                std::unordered_map<Int64, RetainedSnapshot> retained;
                bool applied = false;
                auto snapshots = current_metadata->getArray(Iceberg::f_snapshots);
                for (UInt32 i = 0; snapshots && i < snapshots->size(); ++i)
                {
                    auto snapshot = snapshots->getObject(i);
                    const auto id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
                    std::optional<Int64> snapshot_parent;
                    if (snapshot->has(Iceberg::f_parent_snapshot_id) && !snapshot->isNull(Iceberg::f_parent_snapshot_id))
                        snapshot_parent = snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
                    /// Our snapshot, or a snapshot built on top of it, is in the table: some request of ours was applied.
                    if (id == our_id || snapshot_parent == our_id)
                        applied = true;
                    /// Snapshots written before the table was upgraded to v2 have no sequence number, which reads as 0.
                    const Int64 sequence_number = snapshot->has(Iceberg::f_metadata_sequence_number)
                        ? snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number)
                        : 0;
                    retained.emplace(id, RetainedSnapshot{snapshot_parent, sequence_number});
                }
                if (applied)
                {
                    LOG_WARNING(
                        log,
                        "The catalog rejected the commit of snapshot {}, but it or a snapshot built on it is in the table: "
                        "an earlier attempt of the same request was applied",
                        our_id);
                    return CommitOutcome::Committed;
                }

                /// Sequence numbers grow with every commit, so only a snapshot whose lineage loses a link above ours may descend from ours.
                for (const auto & [_, retained_snapshot] : retained)
                {
                    const RetainedSnapshot * node = &retained_snapshot;
                    for (size_t step = 0; node->sequence_number > our_sequence_number && node->parent; ++step)
                    {
                        auto it = retained.find(*node->parent);
                        if (it == retained.end() || step >= retained.size())
                            return CommitOutcome::Unknown;
                        node = &it->second;
                    }
                }

                /// A conflict is proven only when main's retained lineage reaches our parent through another snapshot.
                if (current_metadata->has(Iceberg::f_current_snapshot_id))
                {
                    auto id = current_metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
                    for (size_t step = 0; step < retained.size() && id != parent_id; ++step)
                    {
                        auto it = retained.find(id);
                        if (it == retained.end() || !it->second.parent)
                            break;
                        if (*it->second.parent == parent_id)
                        {
                            LOG_INFO(log, "Metadata commit conflict detected via catalog");
                            return CommitOutcome::Conflict;
                        }
                        id = *it->second.parent;
                    }
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to check the rejected catalog commit");
            }
            return CommitOutcome::Unknown;
        }
    }
    return CommitOutcome::Committed;
}

[[noreturn]] void throwUnknownCommitStatus(const IcebergPathResolver & path_resolver, const GeneratedMetadataFileWithInfo & metadata_info, const String & cause)
{
    throw Exception(
        ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
        "It is unknown whether the Iceberg commit of {} was applied{}{}. The files written for it are kept, "
        "`remove_orphan_files` removes them if it was not",
        path_resolver.resolve(metadata_info.path),
        cause.empty() ? "" : ": ",
        cause);
}

void removeObjectsIfExist(const std::vector<IcebergPathFromMetadata> & paths, const IcebergPathResolver & path_resolver, ObjectStoragePtr object_storage, LoggerPtr log)
{
    for (const auto & path : paths)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(path)));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to remove {} during cleanup", path.serialize()));
        }
    }
}

}

static bool writeConsolidatedManifestFile(
    int metadata_version,
    const String & metadata_file_path,
    Poco::JSON::Object::Ptr metadata_object,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage, ContextPtr context,
    SharedHeader sample_block_,
    String write_format,
    CompressionMethod compression_method,
    const DataLakeStorageSettings & data_lake_settings,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id)
{
    auto log = getLogger("IcebergManifestConsolidation");

    // Derive current snapshot info directly from the metadata file.
    if (!metadata_object->has(Iceberg::f_current_snapshot_id))
    {
        LOG_INFO(log, "No current snapshot found, skipping manifest consolidation");
        return true;
    }
    Int64 current_snapshot_id_val = metadata_object->getValue<Int64>(Iceberg::f_current_snapshot_id);
    if (current_snapshot_id_val < 0)
    {
        LOG_INFO(log, "No current snapshot found, skipping manifest consolidation");
        return true;
    }

    Int64 current_snapshot_id = current_snapshot_id_val;
    String current_manifest_list_path;

    {
        auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            if (snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == current_snapshot_id)
            {
                current_manifest_list_path = snapshot->getValue<String>(Iceberg::f_manifest_list);
                break;
            }
        }
    }

    if (current_manifest_list_path.empty())
    {
        LOG_INFO(log, "No current snapshot found, skipping manifest consolidation");
        return true;
    }

    LOG_INFO(log, "Writing consolidated manifest file from current snapshot {}", current_snapshot_id);

    auto current_schema_id = metadata_object->getValue<Int64>(Iceberg::f_current_schema_id);
    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(static_cast<UInt32>(i));
            break;
        }
    }

    if (!current_schema)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg metadata does not contain a schema entry matching current-schema-id {}",
            current_schema_id);

    PartitionSpecResolver spec_resolver(metadata_object, persistent_table_components, context);

    // Collect data files grouped by (partition spec-id, schema-id, partition key)
    ManifestFilesGroups partitions_map;

    // Collect live data files from the current snapshot only; iterating older snapshots would resurrect deleted files.
    size_t total_data_files = 0;
    // Only data manifests are consolidated; delete-file manifests are carried forward unchanged so deleted rows do not reappear.
    std::unordered_set<String> delete_manifest_paths;
    /// The data manifests of the current snapshot, i.e. exactly the ones a rewrite would replace.
    std::vector<IcebergPathFromMetadata> data_manifest_paths;

    auto current_manifest_list = getManifestList(
        object_storage, persistent_table_components, context, IcebergPathFromMetadata::deserialize(current_manifest_list_path), log);

    for (const auto & manifest_file : current_manifest_list)
    {
        if (manifest_file.content_type == ManifestFileContentType::DELETE)
        {
            delete_manifest_paths.insert(manifest_file.manifest_file_path.serialize());
            continue;
        }
        data_manifest_paths.push_back(manifest_file.manifest_file_path);

        auto files_handle = getManifestFileEntriesHandle(
            object_storage, persistent_table_components, context, log, manifest_file, static_cast<Int32>(current_schema_id));

        for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
            if (addExistingDataFile(partitions_map, manifest_file, *data_file))
                ++total_data_files;
    }

    /// Not a single live data file was collected: every DATA manifest still listed by the current snapshot holds
    /// only deleted entries (e.g. every row was deleted). There is nothing to consolidate, and a rewrite would ask
    /// the Avro writer to close a manifest list that was never written to, dereferencing a null stream.
    /// This is not the "unpartitioned table" case: `partition_key` always carries the source spec-id and schema-id,
    /// so an unpartitioned table with live files still forms one group and is consolidated as before.
    if (total_data_files == 0)
    {
        LOG_INFO(log, "No live data files in the current snapshot ({} data manifests hold only deleted entries); nothing to do",
                 data_manifest_paths.size());
        return true;
    }

    /// Data manifests already optimally consolidated (at most one per partition): rewriting cannot reduce the count, so report success.
    if (partitions_map.size() >= data_manifest_paths.size())
    {
        LOG_INFO(log, "Manifests already optimally consolidated ({} data manifests, {} unique partitions); nothing to do",
                 data_manifest_paths.size(), partitions_map.size());
        return true;
    }

    const auto & path_resolver = persistent_table_components.path_resolver;

    /// Checked only here, past the early returns above: those leave every manifest exactly as it is, so for a table that
    /// needs no rewrite at all - in particular an emptied one, whose deleted-only manifests are never read again - there
    /// is no encryption metadata to lose and reporting success must not turn into a hard error.
    checkManifestsHaveNoKeyMetadata(data_manifest_paths, "OPTIMIZE TABLE ... MANIFEST", path_resolver, object_storage, context, log);

    // Create file name generator for new metadata files
    FileNamesGenerator generator(
        path_resolver.getTableLocation(),
        false,
        compression_method,
        write_format);
    generator.setVersion(metadata_version + 1);

    MetadataGenerator metadata_generator(metadata_object);
    auto generated_metadata_info = generator.generateMetadataPathWithInfo();

    // Manifest-only rewrite: use a snapshot type that carries all total-* counters forward unchanged, since passing deltas would inflate the totals.
    auto new_snapshot = metadata_generator.generateManifestOnlySnapshot(
        generator,
        generated_metadata_info.path,
        current_snapshot_id);

    // Write one manifest file per (partition spec, partition value) group.
    std::vector<IcebergPathFromMetadata> consolidated_manifest_paths;
    std::vector<Int64> manifest_entry_sizes;
    /// Parallel to consolidated_manifest_paths: existing (not added) file/row counts, since the referenced data files already exist.
    std::vector<ManifestListEntryCounts> existing_entry_counts;
    /// Parallel to consolidated_manifest_paths: each manifest's partition spec-id.
    std::vector<Int64> entry_partition_spec_ids;
    /// Parallel to consolidated_manifest_paths: each manifest's partition fields (value + type), used to recompute the manifest-list `partitions` summary.
    std::vector<std::vector<std::pair<Field, DataTypePtr>>> entry_partition_summaries;

    /// Cleanup for both commit conflict and exceptions; paths are tracked before writeObject so partially-created objects are removed (removeObjectIfExists tolerates missing objects).
    std::vector<IcebergPathFromMetadata> written_paths;
    auto cleanup = [&]()
    {
        written_paths.push_back(new_snapshot.manifest_list_path);
        removeObjectsIfExist(written_paths, path_resolver, object_storage, log);
    };

    try
    {
        for (auto & [partition_key, pd] : partitions_map)
        {
            auto manifest = writeManifestFilesGroup(
                partition_key,
                pd,
                spec_resolver,
                metadata_object,
                new_snapshot.snapshot,
                generator,
                sample_block_,
                write_format,
                path_resolver,
                object_storage,
                context,
                written_paths,
                log);
            consolidated_manifest_paths.push_back(manifest.path);
            manifest_entry_sizes.push_back(manifest.size);
            existing_entry_counts.push_back(manifest.counts);
            entry_partition_spec_ids.push_back(manifest.partition_spec_id);
            entry_partition_summaries.push_back(std::move(manifest.partition_summary));
        }

        // Create manifest list pointing to all per-partition manifest files
        auto storage_manifest_list_path = path_resolver.resolve(new_snapshot.manifest_list_path);
        LOG_INFO(log, "Creating manifest list with {} partition manifest(s): {}",
                 consolidated_manifest_paths.size(), storage_manifest_list_path);

        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(storage_manifest_list_path),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        generateManifestList(
            path_resolver,
            metadata_object,
            object_storage,
            context,
            consolidated_manifest_paths,
            new_snapshot.snapshot,
            manifest_entry_sizes,
            *buffer_manifest_list,
            Iceberg::FileContentType::DATA,
            false,
            /* per_entry_content_types */ {},
            existing_entry_counts,
            /* carry_forward_manifest_paths */ delete_manifest_paths,
            /* entry_partition_spec_ids */ entry_partition_spec_ids,
            /* entry_partition_summaries */ entry_partition_summaries);
        buffer_manifest_list->finalize();
    }
    catch (...)
    {
        cleanup();
        throw;
    }

    CommitOutcome outcome = CommitOutcome::Unknown;
    try
    {
        outcome = commitCompactedMetadata(
            metadata_object,
            generated_metadata_info,
            generator,
            new_snapshot.snapshot,
            path_resolver,
            persistent_table_components,
            metadata_file_path,
            object_storage,
            context,
            data_lake_settings,
            catalog,
            table_id,
            log);
    }
    catch (const Exception & e)
    {
        /// Thrown before anything was written.
        if (e.code() == ErrorCodes::UNSUPPORTED_METHOD)
        {
            cleanup();
            throw;
        }
        throwUnknownCommitStatus(path_resolver, generated_metadata_info, getCurrentExceptionMessage(false));
    }
    catch (...)
    {
        throwUnknownCommitStatus(path_resolver, generated_metadata_info, getCurrentExceptionMessage(false));
    }

    switch (outcome)
    {
        case CommitOutcome::Committed:
            break;
        case CommitOutcome::Conflict:
            LOG_INFO(log, "Cleaning up the files of the conflicting commit");
            cleanup();
            return false;
        case CommitOutcome::Unknown:
            throwUnknownCommitStatus(path_resolver, generated_metadata_info, "");
    }

    LOG_INFO(log, "Successfully created {} partition manifest file(s) covering {} data files",
             consolidated_manifest_paths.size(), total_data_files);
    return true;
}

void compactIcebergManifests(
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage_,
    const DataLakeStorageSettings & data_lake_settings,
    SharedHeader sample_block_,
    ContextPtr context_,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id)
{
    auto log = getLogger("IcebergManifestCompaction");
    LOG_INFO(log, "Starting manifest-only compaction for Iceberg table");

    const size_t min_count_to_compact = context_->getSettingsRef()[DB::Setting::iceberg_manifest_min_count_to_compact];

    for (size_t attempt = 0; attempt < MAX_COMPACTION_RETRIES; ++attempt)
    {
        if (attempt > 0)
            LOG_INFO(log, "Retrying manifest compaction (attempt {}/{})", attempt + 1, MAX_COMPACTION_RETRIES);

        const auto [metadata_version, metadata_file_path, _] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage_,
            catalog,
            table_id.getTableName(),
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context_,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* ignore_metadata_pointer_overrides */ true);

        auto metadata_object = getMetadataJSONObject(
            metadata_file_path,
            object_storage_,
            persistent_table_components.metadata_cache,
            context_,
            log,
            persistent_table_components.metadata_compression_method,
            persistent_table_components.table_uuid);

        /// Validate the format version on the freshly-fetched metadata (before the threshold early-return), since the table may have been upgraded to v3 by another writer after this table object was created.
        const Int32 format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);
        if (format_version < 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "OPTIMIZE TABLE ... MANIFEST is supported only for Iceberg format_version 2.");
        if (format_version >= 3)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "OPTIMIZE TABLE ... MANIFEST is not yet supported for Iceberg format-version 3: "
                "row-lineage 'first_row_id' round-trip is not implemented");

        /// Cheap pre-check: read just the current manifest list to decide whether the table is above the configured threshold.
        if (!isCurrentManifestListAboveThreshold(
                metadata_object, persistent_table_components, object_storage_, context_, min_count_to_compact))
        {
            LOG_INFO(log, "Manifest compaction is not needed (manifest list is within threshold {})",
                     min_count_to_compact);
            return;
        }

        if (writeConsolidatedManifestFile(
                metadata_version,
                metadata_file_path,
                metadata_object,
                persistent_table_components,
                object_storage_,
                context_,
                sample_block_,
                write_format,
                persistent_table_components.metadata_compression_method,
                data_lake_settings,
                catalog,
                table_id))
        {
            // Invalidate metadata cache so the next reader picks up the new state
            if (persistent_table_components.metadata_cache)
            {
                persistent_table_components.metadata_cache->remove(persistent_table_components.table_path);
                if (persistent_table_components.table_uuid)
                    persistent_table_components.metadata_cache->remove(*persistent_table_components.table_uuid);
            }
            LOG_INFO(log, "Successfully compacted manifest list");
            return;
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest compaction failed to commit after {} attempts",
                MAX_COMPACTION_RETRIES);
}

namespace
{

/// A data file rewritten with its position deletes applied; `path` is empty when no row survived.
struct RewrittenDataFile
{
    IcebergPathFromMetadata path;
    UInt64 rows = 0;
    UInt64 bytes = 0;
    DataFileStatisticsPtr statistics;
};

void applyActions(const ExpressionActions & actions, Chunk & chunk, const Block & input_header, const Block & output_header)
{
    auto block = input_header.cloneWithColumns(chunk.detachColumns());
    actions.execute(block);
    Columns columns;
    columns.reserve(output_header.columns());
    for (const auto & column : output_header)
        columns.push_back(block.getByName(column.name).column->convertToFullColumnIfConst());
    chunk.setColumns(std::move(columns), block.rows());
}

/// Reads `source` exactly as SELECT does - in the schema it was written with, identity-partition columns from the manifest, then its
/// position deletes, then the transformation to the current schema - and writes the surviving rows into one new data file.
RewrittenDataFile rewriteDataFile(
    const IcebergDataObjectInfoPtr & source,
    Int32 current_schema_id,
    Poco::JSON::Array::Ptr current_schema_fields,
    SharedHeader current_header,
    FileNamesGenerator & generator,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    const String & write_format,
    LoggerPtr log)
{
    auto & schema_processor = *persistent_table_components.schema_processor;
    const Int32 source_schema_id = source->info.underlying_format_read_schema_id;

    Block source_header;
    for (const auto & [name, type] : *schema_processor.getClickHouseTableSchemaById(source_schema_id))
        source_header.insert({type->createColumn(), type, name});
    auto shared_source_header = std::make_shared<const Block>(source_header);

    ExpressionActionsPtr identity_partition_actions;
    if (auto dag = buildIdentityPartitionColumnsDag(source_header, source->info.identity_partition_columns))
        identity_partition_actions = std::make_shared<ExpressionActions>(std::move(*dag));

    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), /*num_streams_=*/1);
    auto delete_transform = std::make_shared<IcebergBitmapPositionDeleteTransform>(
        shared_source_header, source, object_storage, format_settings, parser_shared_resources, context);

    ExpressionActionsPtr schema_actions;
    if (source_schema_id != current_schema_id)
        if (auto dag = schema_processor.getSchemaTransformationDagByIds(source_schema_id, current_schema_id))
            schema_actions = std::make_shared<ExpressionActions>(dag->clone());

    const String source_format = source->getFileFormat().value_or(write_format);
    RelativePathWithMetadata relative_path(source->getPath());
    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.random_access = FormatFactory::instance().checkIfFormatIsRandomAccessInput(source_format, context);
    auto read_buffer = createReadBuffer(relative_path, object_storage, context, log, read_settings);

    FormatSettings read_format_settings = format_settings ? *format_settings : getFormatSettings(context);
    /// The output keeps the source's row order, so its `sort_order_id` stays true.
    read_format_settings.parquet.preserve_order = true;
    auto input_format = FormatFactory::instance().getInput(
        source_format,
        *read_buffer,
        source_header,
        context,
        8192,
        read_format_settings,
        parser_shared_resources,
        std::make_shared<FormatFilterInfo>(nullptr, context, schema_processor.getColumnMapperById(source_schema_id), nullptr, nullptr),
        true /* is_remote_fs */,
        chooseCompressionMethod(source->getPath(), "auto"),
        false);

    MultipleFileWriter writer(
        std::numeric_limits<UInt64>::max(),
        std::numeric_limits<UInt64>::max(),
        current_schema_fields,
        generator,
        persistent_table_components.path_resolver,
        object_storage,
        context,
        format_settings,
        write_format,
        current_header);

    RewrittenDataFile result;
    try
    {
        while (auto chunk = input_format->read())
        {
            if (identity_partition_actions)
                applyActions(*identity_partition_actions, chunk, source_header, source_header);
            delete_transform->transform(chunk);
            if (schema_actions)
                applyActions(*schema_actions, chunk, source_header, *current_header);
            if (chunk.getNumRows() == 0)
                continue;
            writer.consume(chunk);
            result.rows += chunk.getNumRows();
        }

        if (writer.getDataFiles().empty())
            return result;

        writer.finalize();
        writer.release();

        result.path = writer.getDataFiles().front();
        result.bytes = writer.getDataFileByteCounts().front();
        /// Some storage backends (e.g. Azure) don't track bytes in the write buffer; query the object size.
        if (result.bytes == 0)
            result.bytes = object_storage->getObjectMetadata(persistent_table_components.path_resolver.resolve(result.path), /*with_tags=*/false).size_bytes;
        result.statistics = writer.getPerFileStatistics().front();
    }
    catch (...)
    {
        writer.cancel();
        try
        {
            writer.clearAllDataFiles();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove the partially rewritten data file");
        }
        throw;
    }
    return result;
}

}

void compactIcebergTable(
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id)
{
    auto log = getLogger("IcebergCompaction");
    const auto & path_resolver = persistent_table_components.path_resolver;
    auto & schema_processor = *persistent_table_components.schema_processor;

    /// Rewritten files are reused by later attempts; keyed by the source path, its applied deletes and the target schema.
    using RewriteKey = std::tuple<String, std::vector<String>, Int32>;
    std::map<RewriteKey, RewrittenDataFile> rewritten_files;
    /// The manifests and the manifest list of the current attempt.
    std::vector<IcebergPathFromMetadata> attempt_files;
    bool commit_status_unknown = false;

    auto remove_rewritten_files = [&](const std::set<RewriteKey> & keep_keys)
    {
        for (auto it = rewritten_files.begin(); it != rewritten_files.end();)
        {
            if (keep_keys.contains(it->first))
            {
                ++it;
                continue;
            }
            if (!it->second.path.empty())
                removeObjectsIfExist({it->second.path}, path_resolver, object_storage, log);
            it = rewritten_files.erase(it);
        }
    };

    try
    {
        for (size_t attempt = 0; attempt < MAX_COMPACTION_RETRIES; ++attempt)
        {
            if (attempt > 0)
                LOG_INFO(log, "Retrying data compaction (attempt {}/{})", attempt + 1, MAX_COMPACTION_RETRIES);

            const auto [metadata_version, metadata_file_path, metadata_compression_method] = getLatestMetadataFileAndVersionWithCatalog(
                object_storage,
                catalog,
                table_id.getTableName(),
                persistent_table_components.table_path,
                data_lake_settings,
                persistent_table_components.metadata_cache,
                context,
                log.get(),
                persistent_table_components.table_uuid,
                persistent_table_components.metadata_compression_method,
                /* ignore_metadata_pointer_overrides */ true);

            auto metadata_object = getMetadataJSONObject(
                metadata_file_path,
                object_storage,
                persistent_table_components.metadata_cache,
                context,
                log,
                metadata_compression_method,
                persistent_table_components.table_uuid);

            if (metadata_object->getValue<Int32>(Iceberg::f_format_version) != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Compaction is supported only for format_version 2.");

            /// Without the snapshot -> schema bindings an entry would fall back to its manifest's schema header.
            registerIcebergMetadataSchemas(metadata_object, schema_processor);

            const Int64 current_snapshot_id
                = metadata_object->has(Iceberg::f_current_snapshot_id) && !metadata_object->isNull(Iceberg::f_current_snapshot_id)
                ? metadata_object->getValue<Int64>(Iceberg::f_current_snapshot_id)
                : -1;
            Poco::JSON::Object::Ptr current_snapshot;
            if (current_snapshot_id >= 0)
            {
                auto snapshots = metadata_object->getArray(Iceberg::f_snapshots);
                for (UInt32 i = 0; snapshots && i < snapshots->size(); ++i)
                    if (snapshots->getObject(i)->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == current_snapshot_id)
                        current_snapshot = snapshots->getObject(i);
            }
            if (!current_snapshot)
            {
                LOG_INFO(log, "No current snapshot found, nothing to compact");
                remove_rewritten_files({});
                return;
            }

            const Int32 current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
            auto current_manifest_list = getManifestList(
                object_storage,
                persistent_table_components,
                context,
                IcebergPathFromMetadata::deserialize(current_snapshot->getValue<String>(Iceberg::f_manifest_list)),
                log);

            /// Totals are recomputed from the live entries: the stored ones may be inexact.
            SnapshotSummaryTotals totals;
            std::vector<std::pair<ManifestFileCacheKey, std::vector<ProcessedManifestFileEntryPtr>>> data_manifests;
            std::vector<ProcessedManifestFileEntryPtr> position_deletes;
            std::vector<std::pair<ManifestFileCacheKey, ProcessedManifestFileEntryPtr>> position_delete_entries;
            for (const auto & manifest_file : current_manifest_list)
            {
                auto files_handle = getManifestFileEntriesHandle(
                    object_storage, persistent_table_components, context, log, manifest_file, current_schema_id);
                if (manifest_file.content_type == ManifestFileContentType::DELETE)
                {
                    if (!files_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE).empty())
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Compaction of Iceberg tables with equality deletes is not supported");
                    for (const auto & delete_file : files_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                    {
                        if (delete_file->parsed_entry->isDeletionVector())
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Compaction of Iceberg tables with deletion vectors is not supported");
                        position_deletes.push_back(delete_file);
                        position_delete_entries.emplace_back(manifest_file, delete_file);
                        totals.delete_files += 1;
                        totals.position_deletes += static_cast<UInt64>(delete_file->parsed_entry->record_count);
                        totals.files_size += static_cast<UInt64>(delete_file->parsed_entry->file_size_in_bytes);
                    }
                    continue;
                }

                const auto & data_files = files_handle.getFilesWithoutDeleted(FileContentType::DATA);
                for (const auto & data_file : data_files)
                {
                    totals.data_files += 1;
                    totals.records += static_cast<UInt64>(data_file->parsed_entry->record_count);
                    totals.files_size += static_cast<UInt64>(data_file->parsed_entry->file_size_in_bytes);
                }
                data_manifests.emplace_back(manifest_file, data_files);
            }

            if (position_deletes.empty())
            {
                LOG_INFO(log, "The current snapshot has no position deletes, nothing to compact");
                remove_rewritten_files({});
                return;
            }

            /// "Source" = a live data file at least one position delete applies to, by the rule SELECT applies them with.
            std::sort(position_deletes.begin(), position_deletes.end());
            std::unordered_map<String, IcebergDataObjectInfoPtr> sources;
            std::unordered_map<String, RewriteKey> source_reuse_keys;
            std::unordered_set<String> changed_partitions;
            for (const auto & [manifest_file, data_files] : data_manifests)
            {
                for (const auto & data_file : data_files)
                {
                    const auto & data_file_path = data_file->parsed_entry->file_path_key;
                    if (sources.contains(data_file_path.serialize()))
                        continue;

                    IcebergDataObjectInfoPtr source;
                    std::vector<String> applied_deletes;
                    for (const auto & position_delete : defineDeletesSpan(data_file, position_deletes, /* is_equality_delete */ false, log))
                    {
                        if (!positionDeleteMayReferenceDataFile(*position_delete->parsed_entry, data_file_path))
                            continue;
                        if (!source)
                            source = std::make_shared<IcebergDataObjectInfo>(
                                data_file,
                                path_resolver.resolve(data_file_path),
                                current_schema_id,
                                getIdentityPartitionColumnValues(*data_file, schema_processor));
                        source->addPositionDeleteFile(position_delete, path_resolver.resolve(position_delete->parsed_entry->file_path_key));
                        applied_deletes.push_back(position_delete->parsed_entry->file_path_key.serialize());
                    }
                    if (!source)
                        continue;

                    std::sort(applied_deletes.begin(), applied_deletes.end());
                    source_reuse_keys.emplace(
                        data_file_path.serialize(), RewriteKey{data_file_path.serialize(), std::move(applied_deletes), current_schema_id});
                    sources.emplace(data_file_path.serialize(), std::move(source));

                    String partition_key = std::to_string(manifest_file.partition_spec_id);
                    FieldVisitorDump dump_visitor;
                    for (const auto & value : data_file->normalized_partition_key_value)
                        partition_key += "|" + applyVisitor(dump_visitor, value);
                    changed_partitions.insert(std::move(partition_key));
                }
            }

            std::vector<IcebergPathFromMetadata> affected_manifest_paths;
            for (const auto & [manifest_file, data_files] : data_manifests)
                if (std::any_of(data_files.begin(), data_files.end(),
                        [&](const auto & data_file) { return sources.contains(data_file->parsed_entry->file_path_key.serialize()); }))
                    affected_manifest_paths.push_back(manifest_file.manifest_file_path);
            checkManifestsHaveNoKeyMetadata(affected_manifest_paths, "OPTIMIZE TABLE", path_resolver, object_storage, context, log);

            std::set<RewriteKey> planned_reuse_keys;
            for (const auto & [_, reuse_key] : source_reuse_keys)
                planned_reuse_keys.insert(reuse_key);
            remove_rewritten_files(planned_reuse_keys);

            const bool use_uuid_in_metadata = catalog && catalog->isTransactional();
            FileNamesGenerator generator(path_resolver.getTableLocation(), use_uuid_in_metadata, metadata_compression_method, write_format);
            generator.setVersion(metadata_version + 1);
            if (metadata_object->has(Iceberg::f_properties))
            {
                auto properties = metadata_object->getObject(Iceberg::f_properties);
                if (properties && properties->has("write.data.path"))
                    generator.setDataLocation(properties->getValue<String>("write.data.path"));
            }

            Poco::JSON::Object::Ptr current_schema;
            {
                auto schemas = metadata_object->getArray(Iceberg::f_schemas);
                for (UInt32 i = 0; i < schemas->size(); ++i)
                    if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
                        current_schema = schemas->getObject(i);
            }
            if (!current_schema)
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Iceberg metadata does not contain a schema entry matching current-schema-id {}",
                    current_schema_id);
            Block current_header_block;
            for (const auto & [name, type] : *schema_processor.getClickHouseTableSchemaById(current_schema_id))
                current_header_block.insert({type->createColumn(), type, name});
            auto current_header = std::make_shared<const Block>(std::move(current_header_block));

            SnapshotSummaryUpdateReplace update;
            update.num_partitions = changed_partitions.size();
            update.removed_delete_files = position_deletes.size();
            update.removed_position_delete_files = position_deletes.size();
            update.removed_position_deletes = totals.position_deletes;
            for (const auto & position_delete : position_deletes)
                update.removed_files_size += static_cast<UInt64>(position_delete->parsed_entry->file_size_in_bytes);

            for (const auto & [source_path, source] : sources)
            {
                const auto & reuse_key = source_reuse_keys.at(source_path);
                if (!rewritten_files.contains(reuse_key))
                {
                    auto rewritten = rewriteDataFile(
                        source,
                        current_schema_id,
                        current_schema->getArray(Iceberg::f_fields),
                        current_header,
                        generator,
                        persistent_table_components,
                        object_storage,
                        format_settings,
                        context,
                        write_format,
                        log);
                    rewritten_files.emplace(reuse_key, std::move(rewritten));
                }
                const auto & rewritten = rewritten_files.at(reuse_key);
                update.deleted_data_files += 1;
                update.removed_records += static_cast<UInt64>(source->info.record_count.value_or(0));
                update.removed_files_size += static_cast<UInt64>(source->info.file_size_in_bytes.value_or(0));
                if (rewritten.path.empty())
                    continue;
                update.added_files += 1;
                update.added_records += rewritten.rows;
                update.added_files_size += rewritten.bytes;
            }

            /// Of the parent's extra summary fields only the refresh cursor is carried forward; the others describe the parent's own write.
            SnapshotSummaryExtraFields extra_fields;
            if (current_snapshot->has(Iceberg::f_summary))
            {
                auto parent_summary = SnapshotSummary::fromJSON(
                    *current_snapshot->getObject(Iceberg::f_summary), /*with_extra_fields=*/true, /*require_totals=*/false);
                if (!parent_summary)
                    throw Exception(
                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Cannot use summary of parent snapshot {} for compaction: {}",
                        current_snapshot_id,
                        parent_summary.error());
                const auto & parent_extra_fields = parent_summary->getExtraFields();
                if (auto it = parent_extra_fields.find(Iceberg::f_refresh_cursor); it != parent_extra_fields.end())
                    extra_fields.insert(*it);
            }
            SnapshotSummary summary(update, totals, std::move(extra_fields));

            MetadataGenerator metadata_generator(metadata_object);
            auto metadata_info = generator.generateMetadataPathWithInfo();
            auto new_snapshot = metadata_generator.generateNextMetadata(
                generator,
                metadata_info.path,
                current_snapshot_id,
                update.added_files,
                update.added_records,
                update.added_files_size,
                update.num_partitions,
                /*added_delete_files=*/0,
                /*num_deleted_rows=*/0,
                /*user_defined_snapshot_id=*/std::nullopt,
                /*user_defined_timestamp=*/std::nullopt,
                MetadataGenerator::SnapshotOperation::Replace,
                /*refresh_cursor=*/std::nullopt,
                /*removals=*/{},
                summary);

            /// Every delete manifest is dropped: each live position delete is fully applied once all the data files it can
            /// apply to are rewritten. Replaced files are kept as DELETED entries: writers validate concurrent commits against them.
            std::unordered_set<String> carry_forward_manifest_paths;
            ManifestFilesGroups existing_groups;
            ManifestFilesGroups added_groups;
            ManifestFilesGroups deleted_delete_file_groups;
            for (const auto & [manifest_file, delete_file] : position_delete_entries)
                addExistingDataFile(deleted_delete_file_groups, manifest_file, *delete_file, /*deleted=*/true);
            for (auto & [_, group] : deleted_delete_file_groups)
                group.content_type = Iceberg::FileContentType::POSITION_DELETE;
            for (const auto & [manifest_file, data_files] : data_manifests)
            {
                const bool affected = std::any_of(data_files.begin(), data_files.end(),
                    [&](const auto & data_file) { return sources.contains(data_file->parsed_entry->file_path_key.serialize()); });
                if (!affected)
                {
                    carry_forward_manifest_paths.insert(manifest_file.manifest_file_path.serialize());
                    continue;
                }
                for (const auto & data_file : data_files)
                {
                    const auto source_path = data_file->parsed_entry->file_path_key.serialize();
                    if (!sources.contains(source_path))
                    {
                        addExistingDataFile(existing_groups, manifest_file, *data_file);
                        continue;
                    }
                    addExistingDataFile(existing_groups, manifest_file, *data_file, /*deleted=*/true);
                    const auto & rewritten = rewritten_files.at(source_reuse_keys.at(source_path));
                    if (rewritten.path.empty())
                        continue;
                    auto & group = getManifestFilesGroup(added_groups, manifest_file.partition_spec_id, current_schema_id, data_file->normalized_partition_key_value);
                    if (std::find(group.file_paths.begin(), group.file_paths.end(), rewritten.path) != group.file_paths.end())
                        continue;
                    group.file_paths.push_back(rewritten.path);
                    group.file_metrics.emplace_back(rewritten.rows, rewritten.bytes);
                    group.file_sort_order_ids.push_back(data_file->parsed_entry->sort_order_id);
                    group.file_fresh_statistics.push_back(rewritten.statistics.get());
                }
            }

            PartitionSpecResolver spec_resolver(metadata_object, persistent_table_components, context);
            std::vector<IcebergPathFromMetadata> manifest_paths;
            std::vector<Int64> manifest_sizes;
            std::vector<ManifestListEntryCounts> entry_counts;
            std::vector<Iceberg::FileContentType> entry_content_types;
            std::vector<Int64> entry_partition_spec_ids;
            std::vector<std::vector<std::pair<Field, DataTypePtr>>> entry_partition_summaries;
            for (const auto * groups : {&added_groups, &existing_groups, &deleted_delete_file_groups})
            {
                for (const auto & [group_key, group] : *groups)
                {
                    auto manifest = writeManifestFilesGroup(
                        group_key,
                        group,
                        spec_resolver,
                        metadata_object,
                        new_snapshot.snapshot,
                        generator,
                        current_header,
                        write_format,
                        path_resolver,
                        object_storage,
                        context,
                        attempt_files,
                        log);
                    manifest_paths.push_back(manifest.path);
                    manifest_sizes.push_back(manifest.size);
                    entry_counts.push_back(manifest.counts);
                    entry_content_types.push_back(group.content_type);
                    entry_partition_spec_ids.push_back(manifest.partition_spec_id);
                    entry_partition_summaries.push_back(std::move(manifest.partition_summary));
                }
            }

            attempt_files.push_back(new_snapshot.manifest_list_path);
            {
                auto buffer_manifest_list = object_storage->writeObject(
                    StoredObject(path_resolver.resolve(new_snapshot.manifest_list_path)),
                    WriteMode::Rewrite,
                    std::nullopt,
                    DBMS_DEFAULT_BUFFER_SIZE,
                    context->getWriteSettings());
                generateManifestList(
                    path_resolver,
                    metadata_object,
                    object_storage,
                    context,
                    manifest_paths,
                    new_snapshot.snapshot,
                    manifest_sizes,
                    *buffer_manifest_list,
                    Iceberg::FileContentType::DATA,
                    /* use_previous_snapshots */ false,
                    entry_content_types,
                    entry_counts,
                    carry_forward_manifest_paths,
                    entry_partition_spec_ids,
                    entry_partition_summaries);
                buffer_manifest_list->finalize();
            }

            FailPointInjection::pauseFailPoint(FailPoints::iceberg_compaction_pause_before_metadata_commit);

            CommitOutcome outcome = CommitOutcome::Unknown;
            try
            {
                outcome = commitCompactedMetadata(
                    metadata_object,
                    metadata_info,
                    generator,
                    new_snapshot.snapshot,
                    path_resolver,
                    persistent_table_components,
                    metadata_file_path,
                    object_storage,
                    context,
                    data_lake_settings,
                    catalog,
                    table_id,
                    log);
            }
            catch (const Exception & e)
            {
                /// Thrown before anything was written.
                if (e.code() == ErrorCodes::UNSUPPORTED_METHOD)
                    throw;
                commit_status_unknown = true;
                throwUnknownCommitStatus(path_resolver, metadata_info, getCurrentExceptionMessage(false));
            }
            catch (...)
            {
                commit_status_unknown = true;
                throwUnknownCommitStatus(path_resolver, metadata_info, getCurrentExceptionMessage(false));
            }

            switch (outcome)
            {
                case CommitOutcome::Committed:
                    persistent_table_components.invalidateMetadataCache();
                    LOG_INFO(
                        log,
                        "Compacted {} data files with {} position delete files into {} data files",
                        update.deleted_data_files,
                        update.removed_delete_files,
                        update.added_files);
                    return;
                case CommitOutcome::Conflict:
                    removeObjectsIfExist(attempt_files, path_resolver, object_storage, log);
                    attempt_files.clear();
                    continue;
                case CommitOutcome::Unknown:
                    commit_status_unknown = true;
                    throwUnknownCommitStatus(path_resolver, metadata_info, "");
            }
        }

        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to commit Iceberg data compaction");
    }
    catch (...)
    {
        if (!commit_status_unknown)
        {
            removeObjectsIfExist(attempt_files, path_resolver, object_storage, log);
            remove_rewritten_files({});
        }
        throw;
    }
}

}

#endif
