#include <limits>
#include <map>
#include <optional>
#include <string>
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
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
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
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/String.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorDump.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#if USE_AVRO && !CLICKHOUSE_CLOUD

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int NOT_IMPLEMENTED;
    extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
}

namespace DB::DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace DB::Setting
{
    extern const SettingsUInt64 iceberg_manifest_min_count_to_compact;
    extern const SettingsBool iceberg_insert_single_manifest_for_all_partitions;
}

namespace DB::Iceberg
{

static constexpr size_t MAX_COMPACTION_RETRIES = 100;

/// Outcome of publishing the compacted metadata, which decides what may be deleted afterwards.
enum class CommitResult
{
    /// Published and discoverable: the pre-compaction generation can go.
    Published,
    /// Another writer took the target version; nothing of ours was published, so the rewritten
    /// files are removed and the pre-compaction generation stays.
    Lost,
    /// Published but not confirmed discoverable, or the outcome of the write is unknown. A reader
    /// may resolve either generation, so BOTH are kept and nothing is deleted.
    KeepEverything,
};

using namespace DB;

struct ManifestFilePlan
{
    ManifestFilePlan() = default;

    Iceberg::IcebergPathFromMetadata path;
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_lists_path;

    Iceberg::IcebergPathFromMetadata patched_path;
};

struct DataFilePlan
{
    IcebergDataObjectInfoPtr data_object_info;
    std::shared_ptr<ManifestFilePlan> manifest_list;
    std::shared_ptr<const Iceberg::PartitionSpecification> partition_specification;

    Iceberg::IcebergPathFromMetadata patched_path;
    UInt64 new_records_count = 0;
    UInt64 new_bytes_count = 0;

    /// Statistics and partition value are per DATA FILE, not per manifest: one manifest can pack
    /// files from different partitions, so aggregating here would union bounds across unrelated
    /// files and stamp a single partition on all of them, breaking both kinds of pruning.
    DataFileStatisticsPtr statistics;
};

/// Compaction plan: all data files, the delete files applied to them, and prior metadata.
struct Plan
{
    bool need_optimize = false;
    using PartitionPlan = std::vector<std::shared_ptr<DataFilePlan>>;
    std::vector<PartitionPlan> partitions;
    IcebergHistory history;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Int64> manifest_file_to_first_snapshot;
    /// Original manifest-list entry lineage per manifest path: `added_snapshot_id` of the
    /// snapshot that actually added the manifest. This is taken from the source manifest-list
    /// entries rather than derived from retained history order, because the adding snapshot
    /// may already be expired from table metadata while later snapshots still carry the
    /// manifest forward. Sequence numbers are not preserved here: the rewrite renumbers the
    /// history, so they are remapped onto the new numbering when the manifests are rewritten.
    struct ManifestFileLineage
    {
        Int64 added_snapshot_id = 0;
    };
    std::unordered_map<Iceberg::IcebergPathFromMetadata, ManifestFileLineage> manifest_file_lineage;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::vector<Iceberg::IcebergPathFromMetadata>> manifest_list_to_manifest_files;
    std::unordered_map<Int64, std::vector<std::shared_ptr<DataFilePlan>>> snapshot_id_to_data_files;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<DataFilePlan>> path_to_data_file;
    /// Manifests and manifest lists written by the rewrite, so they can be removed again if the
    /// metadata commit is lost. Never the commit target itself: that name is the winner's.
    std::vector<Iceberg::IcebergPathFromMetadata> generated_metadata_paths;
    /// Set once the compacted metadata is committed. After that the rewritten files are reachable
    /// from it, so they must not be removed even if a later step throws.
    bool metadata_published = false;
    FileNamesGenerator generator;
    Poco::JSON::Object::Ptr initial_metadata_object;

    class ParititonEncoder
    {
    public:
        size_t encodePartition(const Row & row)
        {
            if (auto it = partition_value_to_index.find(row); it != partition_value_to_index.end())
                return it->second;

            partition_value_to_index[row] = partition_values.size();
            partition_values.push_back(row);
            return partition_value_to_index[row];
        }

        const Row & getPartitionValue(size_t partition_index) const { return partition_values.at(partition_index); }

    private:
        struct PartitionValueHasher
        {
            std::hash<String> hasher;
            size_t operator()(const Row & row) const
            {
                size_t result = 0;
                for (const auto & value : row)
                    result ^= hasher(value.dump());
                return result;
            }
        };

        std::unordered_map<Row, size_t, PartitionValueHasher> partition_value_to_index;
        std::vector<Row> partition_values;
    } partition_encoder;
};

/// Requiredness is part of the physical type: the rewrite writes each column under the current
/// schema's optionality, and a mismatch aborts in the Parquet writer.
static void appendRequiredness(
    std::unordered_map<Int64, String> & out, Int64 id, const Poco::JSON::Object::Ptr & container, const char * required_key)
{
    if (container->has(required_key))
        out[id] += container->getValue<bool>(required_key) ? "!" : "?";
}

/// Record `id`'s type signature into `out`, recursing into struct/list/map children.
/// The signature is name- and order-agnostic: a primitive maps to its canonicalized type string,
/// a complex node to its kind, and every nested field/element/key/value is keyed by its own
/// field id.
static void walkTypeNode(
    Int64 id, const Poco::JSON::Object::Ptr & container, const char * type_key, std::unordered_map<Int64, String> & out)
{
    if (!container->isObject(type_key))
    {
        out[id] = canonicalizeTypeSpacing(container->getValue<String>(type_key)); /// primitive leaf
        return;
    }

    auto type_obj = container->getObject(type_key);
    auto kind = type_obj->getValue<String>(Iceberg::f_type);
    out[id] = kind;

    if (kind == Iceberg::f_struct)
    {
        if (auto fields = type_obj->getArray(Iceberg::f_fields))
            for (size_t i = 0; i < fields->size(); ++i)
            {
                auto field = fields->getObject(static_cast<UInt32>(i));
                walkTypeNode(field->getValue<Int64>(Iceberg::f_id), field, Iceberg::f_type, out);
                appendRequiredness(out, field->getValue<Int64>(Iceberg::f_id), field, Iceberg::f_required);
            }
    }
    else if (kind == Iceberg::f_list)
    {
        auto element_id = type_obj->getValue<Int64>(Iceberg::f_element_id);
        walkTypeNode(element_id, type_obj, Iceberg::f_element, out);
        appendRequiredness(out, element_id, type_obj, Iceberg::f_element_required);
    }
    else if (kind == Iceberg::f_map)
    {
        auto value_id = type_obj->getValue<Int64>(Iceberg::f_value_id);
        walkTypeNode(type_obj->getValue<Int64>(Iceberg::f_key_id), type_obj, Iceberg::f_key, out);
        walkTypeNode(value_id, type_obj, Iceberg::f_value, out);
        appendRequiredness(out, value_id, type_obj, Iceberg::f_value_required);
    }
}

/// Build a field-id -> semantic type signature map for one schema's `fields` array, walking
/// recursively into complex types so nested renames/reorders/additions do not change signatures.
static std::unordered_map<Int64, String> schemaFieldTypes(const Poco::JSON::Array::Ptr & fields)
{
    std::unordered_map<Int64, String> id_to_type;
    if (!fields)
        return id_to_type;
    for (size_t i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(static_cast<UInt32>(i));
        auto id = field->getValue<Int64>(Iceberg::f_id);
        walkTypeNode(id, field, Iceberg::f_type, id_to_type);
        appendRequiredness(id_to_type, id, field, Iceberg::f_required);
    }
    return id_to_type;
}

/// Reject compaction unless every schema still reachable from a snapshot has exactly the same field
/// ids and signatures as the current one. The rewrite materializes the current schema into files
/// that older snapshots resolve against their own, so a dropped id, a changed type and an added id
/// are all unsafe. Renames and reorders keep ids and signatures and stay allowed.
void checkCompactionSupportsSchemaEvolution(const Poco::JSON::Object::Ptr & initial_metadata_object)
{
    auto current_schema_id = initial_metadata_object->getValue<Int64>(Iceberg::f_current_schema_id);
    auto schemas = initial_metadata_object->getArray(Iceberg::f_schemas);
    if (!schemas)
        return;

    std::unordered_map<Int64, Poco::JSON::Array::Ptr> schema_fields_by_id;
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(static_cast<UInt32>(i));
        schema_fields_by_id[schema->getValue<Int64>(Iceberg::f_schema_id)] = schema->getArray(Iceberg::f_fields);
    }

    auto current_it = schema_fields_by_id.find(current_schema_id);
    if (current_it == schema_fields_by_id.end())
        return;
    auto current_types = schemaFieldTypes(current_it->second);

    /// Only schema-ids reachable via a snapshot matter for time travel.
    std::unordered_set<Int64> reachable_schema_ids;
    if (auto snapshots = initial_metadata_object->getArray(Iceberg::f_snapshots))
    {
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            if (snapshot->has(Iceberg::f_schema_id) && !snapshot->isNull(Iceberg::f_schema_id))
                reachable_schema_ids.insert(snapshot->getValue<Int64>(Iceberg::f_schema_id));
        }
    }

    for (auto reachable_id : reachable_schema_ids)
    {
        if (reachable_id == current_schema_id)
            continue;
        auto fields_it = schema_fields_by_id.find(reachable_id);
        if (fields_it == schema_fields_by_id.end())
            continue;
        auto old_types = schemaFieldTypes(fields_it->second);
        for (const auto & [field_id, old_type] : old_types)
        {
            auto cur_it = current_types.find(field_id);
            if (cur_it == current_types.end())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg compaction (OPTIMIZE) is not supported after lossy schema evolution: field id {} present in "
                    "schema-id {} (reachable via time travel) was dropped from the current schema-id {}. Compaction would rewrite "
                    "historical files into the current schema and break time travel to older snapshots.",
                    field_id,
                    reachable_id,
                    current_schema_id);
            if (cur_it->second != old_type)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg compaction (OPTIMIZE) is not supported after lossy schema evolution: field id {} changed type between "
                    "schema-id {} (reachable via time travel) and the current schema-id {}. Compaction would rewrite historical "
                    "files into the current schema and break time travel to older snapshots.",
                    field_id,
                    reachable_id,
                    current_schema_id);
        }

        /// The reverse direction: an id the old schema lacks would be written into the rewritten
        /// file and then refused by any reader resolving it against that schema.
        for (const auto & [field_id, current_type] : current_types)
        {
            if (!old_types.contains(field_id))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg compaction (OPTIMIZE) is not supported after schema evolution that added field id {} to the "
                    "current schema-id {}: it is absent from schema-id {}, which is still reachable via time travel, and "
                    "rewriting historical files into the current schema would make them unreadable under that schema.",
                    field_id,
                    current_schema_id,
                    reachable_id);
        }
    }
}

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

static Plan getPlan(
    IcebergHistory snapshots_info,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    const String & write_format,
    ContextPtr context,
    CompressionMethod compression_method)
{
    LoggerPtr log = getLogger("IcebergCompaction::getPlan");

    Plan plan;
    plan.generator = FileNamesGenerator(persistent_table_components.path_resolver.getTableLocation(), false, compression_method, write_format);

    const auto [metadata_version, metadata_file_path, _] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context,
        log.get(),
        persistent_table_components.table_uuid,
        persistent_table_components.metadata_compression_method,
        /* force_fetch_latest_metadata */ true,
        /* ignore_metadata_pointer_overrides */ true);

    Poco::JSON::Object::Ptr initial_metadata_object
        = deepCopy(getMetadataJSONObject(metadata_file_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid));

    /// Exactly version 2: v1 lacks the sequence-number machinery the rewrite relies on, and
    /// a v3 table must not be accepted either -- writeMetadataFiles rebuilds the metadata
    /// from createEmptyMetadataFile, which produces format_version 2, so a v3 table would be
    /// silently downgraded and lose v3-only state such as row lineage (first_row_id /
    /// next_row_id).
    if (initial_metadata_object->getValue<Int32>(Iceberg::f_format_version) != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Compaction is supported only for format_version 2.");

    /// The compacted metadata is the next version of this table, so the generated
    /// `vN.metadata.json` and the `version-hint.text` must advance past the current version.
    plan.generator.setVersion(metadata_version + 1);

    auto current_schema_id = initial_metadata_object->getValue<Int64>(Iceberg::f_current_schema_id);
    auto schemas = initial_metadata_object->getArray(Iceberg::f_schemas);
    Poco::JSON::Array::Ptr current_schema;
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(static_cast<UInt32>(i))->getArray(Iceberg::f_fields);
            break;
        }
    }
    plan.initial_metadata_object = initial_metadata_object;

    /// A snapshot's manifest list also lists the manifests of the earlier snapshots.
    std::vector<ProcessedManifestFileEntryPtr> all_positional_delete_files;
    std::unordered_set<Iceberg::IcebergPathFromMetadata> seen_position_delete_files;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<ManifestFilePlan>> manifest_files;
    for (const auto & snapshot : snapshots_info)
    {
        auto manifest_list = getManifestList(object_storage, persistent_table_components, context, snapshot.manifest_list_path, log);
        for (const auto & manifest_file : manifest_list)
        {
            plan.manifest_list_to_manifest_files[snapshot.manifest_list_path].push_back(manifest_file.manifest_file_path);
            if (!plan.manifest_file_to_first_snapshot.contains(manifest_file.manifest_file_path))
                plan.manifest_file_to_first_snapshot[manifest_file.manifest_file_path] = snapshot.snapshot_id;
            if (!plan.manifest_file_lineage.contains(manifest_file.manifest_file_path))
                plan.manifest_file_lineage[manifest_file.manifest_file_path] = {manifest_file.added_snapshot_id};
            auto files_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log, manifest_file, static_cast<Int32>(current_schema_id));

            if (!manifest_files.contains(manifest_file.manifest_file_path))
            {
                manifest_files[manifest_file.manifest_file_path] = std::make_shared<ManifestFilePlan>();
                manifest_files[manifest_file.manifest_file_path]->path = manifest_file.manifest_file_path;
            }
            manifest_files[manifest_file.manifest_file_path]->manifest_lists_path.push_back(snapshot.manifest_list_path);
            for (const auto & pos_delete_file : files_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                if (seen_position_delete_files.insert(pos_delete_file->parsed_entry->file_path_key).second)
                    all_positional_delete_files.push_back(pos_delete_file);

            for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
            {
                auto partition_index = plan.partition_encoder.encodePartition(data_file->normalized_partition_key_value);
                if (plan.partitions.size() <= partition_index)
                    plan.partitions.push_back({});

                IcebergDataObjectInfoPtr data_object_info = std::make_shared<IcebergDataObjectInfo>(
                    data_file,
                    persistent_table_components.path_resolver.resolve(data_file->parsed_entry->file_path_key),
                    0,
                    Iceberg::getIdentityPartitionColumnValues(*data_file, *persistent_table_components.schema_processor));
                /// Key by the DATA FILE's own path: one manifest can pack many data files, and
                /// keying by manifest path would collapse them onto the first file's plan, so
                /// every other live data file in that manifest would be dropped.
                const auto & data_file_key = data_file->parsed_entry->file_path_key;
                std::shared_ptr<DataFilePlan> data_file_ptr;
                if (auto it = plan.path_to_data_file.find(data_file_key); it == plan.path_to_data_file.end())
                {
                    data_file_ptr = std::make_shared<DataFilePlan>(DataFilePlan{
                        .data_object_info = data_object_info,
                        .manifest_list = manifest_files[manifest_file.manifest_file_path],
                        .partition_specification = data_file->common_partition_specification,
                        .patched_path = plan.generator.generateDataFileName(),
                        .statistics = std::make_shared<DataFileStatistics>(current_schema)});
                    plan.path_to_data_file[data_file_key] = data_file_ptr;
                    plan.partitions[partition_index].push_back(data_file_ptr);
                }
                else
                {
                    data_file_ptr = it->second;
                }
                plan.snapshot_id_to_data_files[snapshot.snapshot_id].push_back(data_file_ptr);
            }
        }
    }

    for (const auto & delete_file : all_positional_delete_files)
    {
        auto partition_index = plan.partition_encoder.encodePartition(delete_file->normalized_partition_key_value);
        if (partition_index >= plan.partitions.size())
            continue;

        if (delete_file->parsed_entry->isDeletionVector())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Compaction of Iceberg tables with deletion vectors is not supported");

        /// `lower/upper_reference_data_file_path` bound the delete file's own `file_path` column,
        /// so a data file outside them has no deleted rows recorded in this delete file.
        const auto & lower = delete_file->parsed_entry->lower_reference_data_file_path;
        const auto & upper = delete_file->parsed_entry->upper_reference_data_file_path;
        const auto & delete_file_specification = *delete_file->common_partition_specification;

        for (auto & data_file : plan.partitions[partition_index])
        {
            if (data_file->data_object_info->info.sequence_number > delete_file->sequence_number)
                continue;

            /// Buckets are keyed by partition value alone, and a position delete applies only within its own partition spec.
            const auto & data_file_specification = *data_file->partition_specification;
            if (data_file_specification < delete_file_specification || delete_file_specification < data_file_specification)
                continue;

            const auto & data_file_path = data_file->data_object_info->info.data_object_file_path_key;
            bool can_contain_data_file_deletes
                = (!lower.has_value() || *lower <= data_file_path)
                && (!upper.has_value() || *upper >= data_file_path);
            if (!can_contain_data_file_deletes)
                continue;

            data_file->data_object_info->addPositionDeleteFile(
                delete_file, persistent_table_components.path_resolver.resolve(delete_file->parsed_entry->file_path_key));
        }
    }
    plan.history = std::move(snapshots_info);
    plan.need_optimize = !all_positional_delete_files.empty();

    /// Check only when files will actually be rewritten, so a no-op OPTIMIZE on an evolved table
    /// stays a no-op. `need_optimize` is known only after the manifest scan above.
    if (plan.need_optimize)
    {
        checkCompactionSupportsSchemaEvolution(initial_metadata_object);

        /// A rewritten file stays listed under its source file's schema-id, and only the Parquet
        /// reader maps columns to that schema by field id; the others match by name.
        if (Poco::toLower(write_format) != "parquet")
        {
            for (const auto & path_and_plan : plan.path_to_data_file)
            {
                Int32 file_schema_id = path_and_plan.second->data_object_info->info.underlying_format_read_schema_id;
                if (file_schema_id != current_schema_id)
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Iceberg compaction (OPTIMIZE) after schema evolution is supported only for the Parquet format, but "
                        "the table writes {}: a data file written under schema-id {} would be rewritten with the columns of "
                        "the current schema-id {} and then read back by the names of its own schema.",
                        write_format,
                        file_schema_id,
                        current_schema_id);
            }
        }
    }

    return plan;
}

/// Compaction reads the metadata file itself, so its `current-schema-id` -- not the schema of the
/// caller's storage snapshot -- is the schema the rewritten files are written under.
static SharedHeader buildCurrentSchemaHeader(
    const Poco::JSON::Object::Ptr & metadata_object, const IcebergSchemaProcessorPtr & schema_processor)
{
    auto current_schema_id = static_cast<Int32>(metadata_object->getValue<Int64>(Iceberg::f_current_schema_id));
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
        schema_processor->addIcebergTableSchema(schemas->getObject(i));

    Block header;
    for (const auto & name_and_type : *schema_processor->getClickHouseTableSchemaById(current_schema_id))
        header.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});
    return std::make_shared<const Block>(std::move(header));
}

static void writeDataFiles(
    Plan & initial_plan,
    SharedHeader sample_block,
    ObjectStoragePtr object_storage,
    const IcebergPathResolver & path_resolver,
    const IcebergSchemaProcessorPtr & schema_processor,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    const String & write_format,
    CompressionMethod write_compression_method)
{
    auto current_schema_id = static_cast<Int32>(initial_plan.initial_metadata_object->getValue<Int64>(Iceberg::f_current_schema_id));

    ColumnMapperPtr column_mapper;
    {
        auto schemas = initial_plan.initial_metadata_object->getArray(Iceberg::f_schemas);
        for (size_t i = 0; i < schemas->size(); ++i)
        {
            auto schema_object = schemas->getObject(static_cast<UInt32>(i));
            /// Make every historical schema known to the processor so that a data file
            /// written under an older schema can be remapped to the current one below.
            /// Reading a data file only registers that file's own schema, so the current
            /// schema (and any other evolution step) may otherwise be missing here.
            schema_processor->addIcebergTableSchema(schema_object);
            if (schema_object->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
            {
                column_mapper = createColumnMapper(schema_object);
                const auto & metadata_object = initial_plan.initial_metadata_object;
                if (metadata_object->has(Iceberg::f_last_column_id) && !metadata_object->isNull(Iceberg::f_last_column_id))
                    column_mapper->setLastAssignedFieldId(metadata_object->getValue<Int64>(Iceberg::f_last_column_id));
            }
        }
    }

    for (auto & [_, data_file] : initial_plan.path_to_data_file)
    {
        /// A file written under an older schema must be read by its OWN schema and then remapped
        /// to the current one, as the normal read path does; reading it by current-schema names
        /// would yield DEFAULT/NULL values or fail outright.
        const Int32 file_schema_id = data_file->data_object_info->info.underlying_format_read_schema_id;
        const bool schema_changed = file_schema_id != current_schema_id;

        SharedHeader reader_header = sample_block;
        ColumnMapperPtr reader_column_mapper = column_mapper;
        std::shared_ptr<ExpressionActions> schema_transform;
        if (schema_changed)
        {
            auto initial_schema = schema_processor->getClickHouseTableSchemaById(file_schema_id);
            Block old_header;
            for (const auto & [name, type] : *initial_schema)
                old_header.insert({type->createColumn(), type, name});
            reader_header = std::make_shared<const Block>(std::move(old_header));
            /// The file's field ids must resolve to the names `reader_header` requests.
            reader_column_mapper = schema_processor->getColumnMapperById(file_schema_id);

            if (auto transform_dag = schema_processor->getSchemaTransformationDagByIds(file_schema_id, current_schema_id))
                schema_transform = std::make_shared<ExpressionActions>(transform_dag->clone());
        }

        /// The transform requires `ChunkInfoRowNumbers` in every chunk even when it has nothing
        /// to delete, and only the Parquet input formats attach it. Data files with attached
        /// position deletes are guaranteed to be Parquet by `addPositionDeleteFile`, but a data
        /// file without them (e.g. an ORC file newer than all position deletes) may be in any
        /// format, so the transform must be skipped for it. Build it against `reader_header` (the
        /// file's own, possibly-older schema) so position deletes are applied before the remap.
        std::shared_ptr<IcebergBitmapPositionDeleteTransform> delete_file_transform;
        if (data_file->data_object_info->info.hasPositionDeletes())
            delete_file_transform = std::make_shared<IcebergBitmapPositionDeleteTransform>(
                reader_header,
                data_file->data_object_info,
                object_storage,
                format_settings,
                // todo make compaction using same FormatParserSharedResources
                std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), 1),
                context);

        RelativePathWithMetadata relative_path(data_file->data_object_info->getPath());
        /// A data file may be Parquet/ORC/Avro; only the ones that will actually seek to a footer at
        /// the tail should skip the generic from-start prefetch.
        auto read_settings = context->getReadSettings();
        read_settings.remote_fs_settings.random_access = FormatFactory::instance().checkIfFormatIsRandomAccessInput(
            data_file->data_object_info->getFileFormat().value_or(write_format), context);
        auto read_buffer = createReadBuffer(relative_path, object_storage, context, getLogger("IcebergCompaction"), read_settings);

        const Settings & settings = context->getSettingsRef();
        auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(
            settings,
            /*num_streams_=*/1);

        auto input_format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, reader_column_mapper, nullptr, nullptr);
        input_format_filter_info->need_row_numbers = delete_file_transform != nullptr;
        auto input_format = FormatFactory::instance().getInput(
            data_file->data_object_info->getFileFormat().value_or(write_format),
            *read_buffer,
            *reader_header,
            context,
            8192,
            format_settings,
            parser_shared_resources,
            input_format_filter_info,
            true /* is_remote_fs */,
            chooseCompressionMethod(data_file->data_object_info->getPath(), toContentEncodingName(write_compression_method)),
            false);

        auto write_buffer = object_storage->writeObject(
            StoredObject(path_resolver.resolve(data_file->patched_path)),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        FormatFilterInfoPtr output_format_filter_info
            = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
        auto output_format = FormatFactory::instance().getOutputFormat(
            write_format, *write_buffer, *sample_block, context, format_settings, output_format_filter_info);

        while (true)
        {
            auto chunk = input_format->read();
            if (chunk.empty())
                break;

            /// Position deletes index the original file, so deletes must be applied against the
            /// old-schema header they were built with BEFORE the rows are remapped. Null when the
            /// data file has no attached position deletes.
            if (delete_file_transform)
                delete_file_transform->transform(chunk);

            if (schema_transform)
            {
                auto block = reader_header->cloneWithColumns(chunk.getColumns());
                schema_transform->execute(block);
                chunk.setColumns(block.getColumns(), block.rows());
            }

            /// Statistics are accumulated per DATA FILE (not per manifest) on the final
            /// current-schema chunk, so each rewritten file carries its own bounds/counts.
            data_file->statistics->update(chunk);
            data_file->new_records_count += chunk.getNumRows();
            ColumnsWithTypeAndName columns_with_types_and_name;
            for (size_t i = 0; i < sample_block->columns(); ++i)
            {
                ColumnWithTypeAndName column(chunk.getColumns()[i], sample_block->getDataTypes()[i], sample_block->getNames()[i]);
                columns_with_types_and_name.push_back(std::move(column));
            }
            auto block = Block(columns_with_types_and_name);
            output_format->write(block);
        }
        output_format->flush();
        output_format->finalize();
        data_file->statistics->addColumnSizesOnDisk(output_format->getColumnSizesOnDisk(), *sample_block);
        write_buffer->finalize();
        auto file_bytes = write_buffer->count();
        if (file_bytes == 0 && !data_file->patched_path.empty())
        {
            /// Some storage backends (e.g. Azure) don't track bytes in the write buffer; query the object size.
            auto obj_metadata = object_storage->getObjectMetadata(path_resolver.resolve(data_file->patched_path), /*with_tags=*/false);
            file_bytes = obj_metadata.size_bytes;
        }
        data_file->new_bytes_count = file_bytes;
    }
}

/// Whether any entry of the manifest has per-file `key_metadata` (an encrypted data file), which a manifest-only rewrite cannot preserve.
static bool manifestHasKeyMetadata(
    const IcebergPathFromMetadata & manifest_path,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    LoggerPtr log)
{
    RelativePathWithMetadata object_info(path_resolver.resolve(manifest_path));
    auto buf = createReadBuffer(object_info, object_storage, context, log);
    AvroForIcebergDeserializer deserializer(std::move(buf), manifest_path, getFormatSettings(context));
    if (!deserializer.hasPath(Iceberg::c_data_file_key_metadata))
        return false;
    for (size_t row = 0; row < deserializer.rows(); ++row)
        if (!deserializer.getValueFromRowByName(row, Iceberg::c_data_file_key_metadata).isNull())
            return true;
    return false;
}

/// Carries the column statistics of an existing manifest entry over verbatim, for a manifest-only rewrite.
static DataFileColumnStatistics getColumnStatisticsForRewrite(const ParsedManifestFileEntry & entry)
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
    }
    /// Bounds stay the raw serialized bytes, so they round-trip whatever the column type.
    for (const auto & [field_id, bounds] : entry.value_bounds)
    {
        if (!bounds.first.isNull())
            stats.lower_bounds.emplace_back(field_id, bounds.first.safeGet<String>());
        if (!bounds.second.isNull())
            stats.upper_bounds.emplace_back(field_id, bounds.second.safeGet<String>());
    }
    return stats;
}

/// Lineage of an existing manifest entry with the values inherited from its manifest resolved, since an EXISTING entry requires them non-null.
static DataFileEntryLineage getEntryLineageForRewrite(const ParsedManifestFileEntry & entry, const ManifestFileCacheKey & manifest)
{
    DataFileEntryLineage lineage;
    lineage.added_snapshot_id = entry.parsed_snapshot_id.value_or(manifest.added_snapshot_id);
    lineage.sequence_number = entry.parsed_sequence_number.value_or(manifest.added_sequence_number);
    /// It can differ from the data sequence number, and when null it inherits the manifest's sequence number too.
    lineage.file_sequence_number = entry.parsed_file_sequence_number.value_or(manifest.added_sequence_number);
    return lineage;
}

static bool writeConsolidatedManifestFile(
    int metadata_version,
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

    auto partitions_specs = metadata_object->getArray(f_partition_specs);

    /// After partition evolution each manifest must be rewritten under the spec its source files used, and after schema evolution
    /// (e.g. widening `decimal(P, S)`) its partition values must be encoded under the schema its source files were written with;
    /// resolve and cache spec info per (spec-id, schema-id).
    struct ResolvedPartitionSpec
    {
        Poco::JSON::Object::Ptr spec;
        std::vector<String> partition_columns;
        DataTypes partition_types;
    };
    std::map<std::pair<Int32, Int32>, ResolvedPartitionSpec> resolved_specs;
    auto resolve_partition_spec = [&](Int32 spec_id, Int32 files_schema_id) -> const ResolvedPartitionSpec &
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
        if (!spec_sample_block && files_schema_id != static_cast<Int32>(current_schema_id))
        {
            schema_id_for_spec = static_cast<Int32>(current_schema_id);
            spec_sample_block = build_sample_block(schema_id_for_spec);
        }
        if (!spec_sample_block)
        {
            for (UInt32 i = 0; i < schemas->size(); ++i)
            {
                Int32 candidate_id = schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id);
                if (candidate_id == files_schema_id || candidate_id == static_cast<Int32>(current_schema_id))
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
    };

    /// Return the raw metadata schema object for a given schema-id, used as the verbatim Avro `schema` header of a rewritten manifest so its data-file bounds resolve under the same schema the files were written with.
    auto get_schema_object_by_id = [&](Int32 schema_id) -> Poco::JSON::Object::Ptr
    {
        for (UInt32 i = 0; i < schemas->size(); ++i)
            if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == schema_id)
                return schemas->getObject(i);
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg metadata does not contain a schema entry matching schema-id {}",
            schema_id);
    };

    /// Live data files grouped by (partition spec-id, schema-id), since a manifest carries a single spec and one `schema` header.
    /// Its entries may belong to any number of partitions, each entry carrying its own partition tuple; unless
    /// `iceberg_insert_single_manifest_for_all_partitions` is set, the files are grouped by partition value as well.
    struct ManifestGroup
    {
        /// The partition spec the source files were written with; the rewritten manifest reuses it.
        Int32 partition_spec_id = 0;
        /// The schema the source files were written under, so the rewritten manifest's `schema` header matches all its entries.
        Int32 schema_id = 0;
        std::vector<IcebergPathFromMetadata> file_paths;
        /// Parallel to file_paths: the source entry's partition tuple.
        std::vector<Row> file_partition_values;
        /// Parallel to file_paths: {record_count, file_size_in_bytes} from the source manifest entry.
        std::vector<std::pair<Int64, Int64>> file_metrics;
        /// Parallel to file_paths: the original file_format, preserved so a rewrite never relabels the file's format.
        std::vector<String> file_formats;
        /// Parallel to file_paths: the source file's per-column statistics, preserved across the rewrite.
        std::vector<DataFileColumnStatistics> file_statistics;
        /// Parallel to file_paths: the source file's sort_order_id, preserved so the rewrite keeps sortedness.
        std::vector<std::optional<Int32>> file_sort_order_ids;
        /// Parallel to file_paths: the source entry's lineage, preserved so each file is emitted as an EXISTING entry retaining its lineage.
        std::vector<DataFileEntryLineage> file_entry_lineage;
        /// Paths already added, so a single manifest does not list the same data file twice.
        std::unordered_set<String> seen_file_paths;
    };

    std::map<std::tuple<Int32, Int32, String>, ManifestGroup> manifest_groups;
    const bool single_manifest_for_all_partitions = context->getSettingsRef()[Setting::iceberg_insert_single_manifest_for_all_partitions];

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
        const Int32 source_partition_spec_id = manifest_file.partition_spec_id;

        auto files_handle = getManifestFileEntriesHandle(
            object_storage, persistent_table_components, context, log, manifest_file, static_cast<Int32>(current_schema_id));

        for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
        {
            const Int32 source_schema_id = data_file->resolved_schema_id;
            /// The dump carries a type tag, so `UInt64` and `Int64` partition values never collide.
            String partition_key;
            if (!single_manifest_for_all_partitions)
                for (const auto & value : data_file->normalized_partition_key_value)
                    partition_key += applyVisitor(FieldVisitorDump(), value) + "|";
            auto & group = manifest_groups[{source_partition_spec_id, source_schema_id, std::move(partition_key)}];
            group.partition_spec_id = source_partition_spec_id;
            group.schema_id = source_schema_id;

            const auto & parsed_entry = *data_file->parsed_entry;
            if (!group.seen_file_paths.insert(parsed_entry.file_path_key.serialize()).second)
                continue;

            group.file_paths.push_back(parsed_entry.file_path_key);
            group.file_partition_values.push_back(data_file->normalized_partition_key_value);
            group.file_metrics.emplace_back(parsed_entry.record_count, parsed_entry.file_size_in_bytes);
            group.file_formats.push_back(parsed_entry.file_format);
            group.file_sort_order_ids.push_back(parsed_entry.sort_order_id);
            group.file_entry_lineage.push_back(getEntryLineageForRewrite(parsed_entry, manifest_file));
            group.file_statistics.push_back(getColumnStatisticsForRewrite(parsed_entry));
            ++total_data_files;
        }
    }

    /// Not a single live data file was collected: every DATA manifest still listed by the current snapshot holds
    /// only deleted entries (e.g. every row was deleted). There is nothing to consolidate, and a rewrite would ask
    /// the Avro writer to close a manifest list that was never written to, dereferencing a null stream.
    /// This is not the "unpartitioned table" case: an unpartitioned table with live files still forms one group.
    if (total_data_files == 0)
    {
        LOG_INFO(log, "No live data files in the current snapshot ({} data manifests hold only deleted entries); nothing to do",
                 data_manifest_paths.size());
        return true;
    }

    /// Data manifests already optimally consolidated (at most one per group): rewriting cannot reduce the count, so report success.
    if (manifest_groups.size() >= data_manifest_paths.size())
    {
        LOG_INFO(log, "Manifests already optimally consolidated ({} data manifests, {} groups); nothing to do",
                 data_manifest_paths.size(), manifest_groups.size());
        return true;
    }

    const auto & path_resolver = persistent_table_components.path_resolver;

    /// A manifest-only rewrite cannot round-trip per-file `key_metadata` (data-file encryption keys), so reject
    /// rather than silently dropping it and making an encrypted table unreadable. This is checked only here,
    /// past the early returns above: those leave every manifest exactly as it is, so for a table that needs no
    /// rewrite at all - in particular an emptied one, whose deleted-only manifests are never read again - there
    /// is no encryption metadata to lose and reporting success must not turn into a hard error.
    for (const auto & data_manifest_path : data_manifest_paths)
        if (manifestHasKeyMetadata(data_manifest_path, path_resolver, object_storage, context, log))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "OPTIMIZE TABLE ... MANIFEST is not supported for Iceberg tables with per-file key_metadata "
                "(encrypted data files): preserving the encryption metadata across a manifest rewrite is not implemented");

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

    // Write one manifest file per group.
    std::vector<IcebergPathFromMetadata> consolidated_manifest_paths;
    std::vector<Int64> manifest_entry_sizes;
    /// Parallel to consolidated_manifest_paths: existing (not added) file/row counts, since the referenced data files already exist.
    std::vector<ManifestListEntryCounts> existing_entry_counts;
    /// Parallel to consolidated_manifest_paths: each manifest's partition spec-id.
    std::vector<Int64> entry_partition_spec_ids;
    /// Parallel to consolidated_manifest_paths: each manifest's manifest-list `partitions` summary.
    std::vector<ManifestPartitionSummary> entry_partition_summaries;

    /// Cleanup for both commit conflict and exceptions; paths are tracked before writeObject so partially-created objects are removed (removeObjectIfExists tolerates missing objects).
    auto cleanup = [&]()
    {
        for (const auto & mp : consolidated_manifest_paths)
        {
            try
            {
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(mp)));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to remove orphaned manifest file during cleanup");
            }
        }
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(new_snapshot.manifest_list_path)));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove orphaned manifest list during cleanup");
        }
    };

    try
    {
        for (auto & [group_key, pd] : manifest_groups)
        {
            auto manifest_path = generator.generateManifestEntryName();
            auto storage_manifest_path = path_resolver.resolve(manifest_path);
            LOG_INFO(log, "Creating manifest file for partition spec {} and schema {}: {} ({} data files)",
                     pd.partition_spec_id, pd.schema_id, storage_manifest_path, pd.file_paths.size());

            /// Track the path before writeObject so `cleanup` removes any object created even if a later step throws.
            consolidated_manifest_paths.push_back(manifest_path);

            auto buffer_manifest = object_storage->writeObject(
                StoredObject(storage_manifest_path),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            std::vector<UInt64> file_row_counts;
            std::vector<UInt64> file_byte_counts;
            file_row_counts.reserve(pd.file_metrics.size());
            file_byte_counts.reserve(pd.file_metrics.size());
            Int64 manifest_existing_rows = 0;
            for (const auto & [record_count, file_size_in_bytes] : pd.file_metrics)
            {
                file_row_counts.push_back(static_cast<UInt64>(record_count));
                file_byte_counts.push_back(static_cast<UInt64>(file_size_in_bytes));
                manifest_existing_rows += record_count;
            }

            /// Lowest data sequence number across this manifest's files; files keep their original sequence numbers, so min_sequence_number must reflect that minimum.
            Int64 manifest_min_sequence_number = std::numeric_limits<Int64>::max();
            for (const auto & lineage : pd.file_entry_lineage)
                manifest_min_sequence_number = std::min(manifest_min_sequence_number, lineage.sequence_number.value_or(0));

            ManifestListEntryCounts consolidated_counts;
            consolidated_counts.files_count = static_cast<Int64>(pd.file_paths.size());
            consolidated_counts.rows_count = manifest_existing_rows;
            consolidated_counts.min_sequence_number = manifest_min_sequence_number;
            existing_entry_counts.push_back(consolidated_counts);

            /// Rewrite this manifest under the partition spec and the schema its source files used, not the default and the current ones.
            const auto & resolved_spec = resolve_partition_spec(pd.partition_spec_id, pd.schema_id);
            entry_partition_spec_ids.push_back(pd.partition_spec_id);

            /// Each partition tuple must match the resolved spec; a mismatch (corrupt or inconsistently-evolved
            /// metadata) would otherwise read past the end of the tuple while writing the consolidated manifest.
            ManifestPartitionSummary partition_summary(resolved_spec.partition_types);
            for (const auto & partition_values : pd.file_partition_values)
            {
                if (partition_values.size() != resolved_spec.partition_columns.size())
                    throw Exception(
                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Iceberg manifest partition tuple has {} values but partition spec {} defines {} columns",
                        partition_values.size(),
                        pd.partition_spec_id,
                        resolved_spec.partition_columns.size());
                partition_summary.add(partition_values);
            }
            entry_partition_summaries.push_back(std::move(partition_summary));

            generateManifestFile(
                metadata_object,
                resolved_spec.partition_columns,
                pd.file_partition_values,
                resolved_spec.partition_types,
                pd.file_paths,
                file_row_counts,
                file_byte_counts,
                std::nullopt,
                sample_block_,
                new_snapshot.snapshot,
                write_format,
                resolved_spec.spec,
                pd.partition_spec_id,
                *buffer_manifest,
                Iceberg::FileContentType::DATA,
                /* user_defined_sequence_number */ std::nullopt,
                /* user_defined_snapshot_id */ std::nullopt,
                /* data_file_formats */ pd.file_formats,
                /* per_file_statistics */ pd.file_statistics,
                /* data_file_sort_order_ids */ pd.file_sort_order_ids,
                /* per_file_entry_lineage */ pd.file_entry_lineage,
                /* schema_to_serialize */ get_schema_object_by_id(pd.schema_id));

            buffer_manifest->finalize();
            Int64 manifest_size = buffer_manifest->count();
            if (manifest_size == 0)
                manifest_size = object_storage->getObjectMetadata(storage_manifest_path, /*with_tags=*/false).size_bytes;
            manifest_entry_sizes.push_back(manifest_size);
        }

        // Create manifest list pointing to all consolidated manifest files
        auto storage_manifest_list_path = path_resolver.resolve(new_snapshot.manifest_list_path);
        LOG_INFO(log, "Creating manifest list with {} consolidated manifest(s): {}",
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

        // Commit: write metadata file with If-None-Match + ETag-based CAS version hint; returns false if another writer claimed this version, so the caller retries.
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata_object, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            auto hint_path = generator.generateVersionHint();
            LOG_INFO(log, "Committing metadata file: {}",
                     path_resolver.resolve(generated_metadata_info.path));

            /// A transactional catalog is the source of truth for the current metadata; for it the storage-side
            /// version hint is irrelevant, so only write the metadata file + version hint when no such catalog owns the table.
            const bool catalog_writes_metadata_file = catalog && catalog->isTransactional();
            if (!catalog_writes_metadata_file
                && !writeMetadataFileAndVersionHint(
                    path_resolver,
                    generated_metadata_info,
                    json_representation,
                    hint_path,
                    object_storage,
                    context,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                LOG_INFO(log, "Metadata commit conflict detected, cleaning up temporary files");
                cleanup();
                return false;
            }

            /// Advance the catalog pointer to the new metadata so catalog-based readers see the compacted snapshot.
            if (catalog)
            {
                auto catalog_filename = path_resolver.resolveForCatalog(generated_metadata_info.path);
                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot.snapshot))
                {
                    LOG_INFO(log, "Metadata commit conflict detected via catalog, cleaning up temporary files");
                    cleanup();
                    return false;
                }
            }
        }
    }
    catch (...)
    {
        cleanup();
        throw;
    }

    LOG_INFO(log, "Successfully created {} consolidated manifest file(s) covering {} data files",
             consolidated_manifest_paths.size(), total_data_files);
    return true;
}

bool overwriteIsPositionDeleteOnly(const SnapshotSummaryUpdateOverwrite & update)
{
    /// Every declared added file and row must be accounted for as a position delete: the
    /// breakdown counters are optional and read as 0 when absent, so their absence is not
    /// evidence. One delete file with deleted rows but no file count is a position delete file.
    return update.added_files == 0 && update.added_records == 0 && update.added_delete_files != 0
        && (update.added_position_delete_files == update.added_delete_files
            || (update.added_position_delete_files == 0 && update.added_position_deletes != 0
                && update.added_delete_files == 1))
        && update.added_equality_delete_files == 0 && update.added_equality_deletes == 0
        && update.deleted_data_files == 0 && update.removed_records == 0 && update.removed_files_size == 0;
}

/// Deep copy a metadata JSON object (Poco shares child arrays/objects by pointer on a shallow
/// copy, so we round-trip through a string to get an independent tree we can safely mutate).
static Poco::JSON::Object::Ptr deepCopyMetadata(const Poco::JSON::Object::Ptr & source)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    source->stringify(oss);
    Poco::JSON::Parser parser;
    return parser.parse(oss.str()).extract<Poco::JSON::Object::Ptr>();
}

[[nodiscard]] std::optional<SnapshotSummaryUpdateAppend> tryGetAppendUpdate(const Iceberg::IcebergHistoryRecord & history_record)
{
    if (!history_record.snapshot_summary)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Missing summary, snapshot={}", history_record.snapshot_id);

    const auto & summary = history_record.snapshot_summary;
    switch (summary->getOperation())
    {
        case SnapshotSummaryOperation::APPEND:
            return summary->getUpdate<SnapshotSummaryUpdateAppend>();
        case SnapshotSummaryOperation::DELETE:
            return std::nullopt;
        case SnapshotSummaryOperation::OVERWRITE: {
            if (overwriteIsPositionDeleteOnly(summary->getUpdate<Iceberg::SnapshotSummaryUpdateOverwrite>()))
                return std::nullopt;
            [[fallthrough]];
        }
        case SnapshotSummaryOperation::REPLACE:
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported snapshot's operation type {}", summary->getOperation());
    }
};

namespace
{

/// Current experimental compact implementation expects snapshots to be either appends or overwrites which has only position deletes
/// Lets force this invariant
void checkIfIcebergHistorySupported(const IcebergHistory & history)
{
    for (const auto & history_record : history)
    {
        auto append = tryGetAppendUpdate(history_record);
        if (append && append->added_files == 0)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Found an append with 0 added_files, snapshot={}", history_record.snapshot_id);
    }
}

}

/// Returns false if the metadata commit lost the race to a concurrent writer (the target
/// `vN.metadata.json` already existed): in that case the compacted metadata was NOT published and
/// the caller must not delete the pre-compaction files.
static CommitResult writeMetadataFiles(
    Plan & plan, const IcebergPathResolver & path_resolver, ObjectStoragePtr object_storage, ContextPtr context, SharedHeader sample_block_, String write_format)
{
    auto log = getLogger("IcebergCompaction");

    /// Deep-copy the source metadata so every table-level field (uuid, location, partition specs,
    /// sort orders, properties, refs, schemas) is carried over; only the snapshot history below is
    /// regenerated.
    auto metadata_object = deepCopyMetadata(plan.initial_metadata_object);

    /// The snapshot history is rebuilt from scratch by the loop below (it reuses the original
    /// snapshot ids via `generateNextMetadata`), so clear the source snapshot state to avoid
    /// duplicating snapshots or leaving `current-snapshot-id` / `last-sequence-number` pointing at
    /// pre-compaction manifest lists that no longer exist.
    metadata_object->set(Iceberg::f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata_object->set(Iceberg::f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata_object->set(Iceberg::f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    /// Both `statistics` and `partition-statistics` point at statistics files inside the
    /// pre-compaction `metadata/` subtree that `clearOldFiles` deletes, and SnapshotFilesTraversal
    /// treats both as reachable files, so both must be cleared to avoid dangling `statistics-path`s.
    metadata_object->set(Iceberg::f_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata_object->set(Iceberg::f_partition_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    metadata_object->set(Iceberg::f_current_snapshot_id, -1);
    metadata_object->set(Iceberg::f_last_sequence_number, 0);
    if (metadata_object->has(Iceberg::f_refs) && metadata_object->getObject(Iceberg::f_refs)->has(Iceberg::f_main))
        metadata_object->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->set(Iceberg::f_metadata_snapshot_id, -1);
    /// The history below is replayed from row 0, so any preserved `next-row-id` must restart there
    /// too. Inert while compaction is restricted to format-version 2, which has no row lineage.
    if (metadata_object->has(Iceberg::f_next_row_id))
        metadata_object->set(Iceberg::f_next_row_id, static_cast<Int64>(0));

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

    /// A snapshot id is bound to its committed schema id on read, and the replay reuses the
    /// original ids, so each snapshot's original schema id must be preserved rather than restamped
    /// with the current one.
    std::unordered_map<Int64, Int32> snapshot_id_to_committed_schema_id;
    {
        auto source_snapshots = plan.initial_metadata_object->getArray(Iceberg::f_snapshots);
        if (source_snapshots)
        {
            for (size_t i = 0; i < source_snapshots->size(); ++i)
            {
                auto snapshot = source_snapshots->getObject(static_cast<UInt32>(i));
                if (snapshot->has(Iceberg::f_schema_id) && !snapshot->isNull(Iceberg::f_schema_id))
                    snapshot_id_to_committed_schema_id[snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id)]
                        = snapshot->getValue<Int32>(Iceberg::f_schema_id);
            }
        }
    }

    MetadataGenerator metadata_generator(metadata_object);
    std::vector<MetadataGenerator::NextMetadataResult> new_snapshots;
    /// Deliberately NOT tracked for cleanup: on a lost commit this name belongs to the winning
    /// writer, and `if-none-match` means a failed commit never leaves a file of ours here.
    auto generated_metadata_info = plan.generator.generateMetadataPathWithInfo();
    std::unordered_map<Int64, Poco::JSON::Object::Ptr> snapshot_id_to_snapshot;

    struct LiveTotals
    {
        Int64 records = 0;
        Int64 data_files = 0;
        Int64 files_size = 0;
    };
    std::unordered_map<Int64, LiveTotals> snapshot_id_to_live_totals;

    for (const auto & history_record : plan.history)
    {
        auto append = tryGetAppendUpdate(history_record);
        if (!append)
        {
            new_snapshots.push_back(MetadataGenerator::NextMetadataResult{});
            continue;
        }

        LiveTotals live;
        for (const auto & data_file : plan.snapshot_id_to_data_files[history_record.snapshot_id])
        {
            live.records += data_file->new_records_count;
            live.files_size += data_file->new_bytes_count;
            ++live.data_files;
        }
        /// The `total-*` counters add the `added_*` values to the parent's totals only when the parent is rewritten too.
        LiveTotals parent;
        if (auto it = snapshot_id_to_live_totals.find(history_record.parent_id); it != snapshot_id_to_live_totals.end())
            parent = it->second;
        snapshot_id_to_live_totals[history_record.snapshot_id] = live;

        auto new_snapshot = metadata_generator.generateNextMetadata(
            plan.generator,
            generated_metadata_info.path,
            history_record.parent_id,
            live.data_files - parent.data_files,
            live.records - parent.records,
            live.files_size - parent.files_size,
            append->num_partitions,
            0,
            0,
            history_record.snapshot_id,
            history_record.made_current_at.value);

        if (auto it = snapshot_id_to_committed_schema_id.find(history_record.snapshot_id);
            it != snapshot_id_to_committed_schema_id.end() && new_snapshot.snapshot)
            new_snapshot.snapshot->set(Iceberg::f_schema_id, it->second);

        new_snapshots.push_back(new_snapshot);
        snapshot_id_to_snapshot[history_record.snapshot_id] = new_snapshot.snapshot;
    }

    /// The replay regenerates only append-like snapshots, so a ref pinned to a skipped one would
    /// outlive its target and leave refs unresolvable. Drop any ref whose snapshot id was not
    /// regenerated; `main` is re-pointed by `generateNextMetadata` and stays valid.
    if (metadata_object->has(Iceberg::f_refs))
    {
        auto refs = metadata_object->getObject(Iceberg::f_refs);
        std::vector<String> refs_to_remove;
        for (const auto & ref_name : refs->getNames())
        {
            auto ref = refs->getObject(ref_name);
            if (!ref || !ref->has(Iceberg::f_metadata_snapshot_id))
                continue;
            auto ref_snapshot_id = ref->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            if (!snapshot_id_to_snapshot.contains(ref_snapshot_id))
                refs_to_remove.push_back(ref_name);
        }
        for (const auto & ref_name : refs_to_remove)
        {
            LOG_WARNING(
                log,
                "Iceberg compaction: pruning ref '{}' whose target snapshot was not regenerated "
                "(delete-only snapshots are not replayed)",
                ref_name);
            refs->remove(ref_name);
        }
    }

    Poco::JSON::Object::Ptr initial_metadata_object = plan.initial_metadata_object;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Iceberg::IcebergPathFromMetadata> manifest_file_renamings;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Int64> manifest_file_sizes;
    /// Per rewritten manifest: actual file/row counts of its content, written into the
    /// manifest-list entries so that metadata row counts stay exact after compaction.
    std::unordered_map<Iceberg::IcebergPathFromMetadata, ManifestListEntryCounts> manifest_file_counts;
    /// Per rewritten manifest of a partitioned table: its manifest-list `partitions` summary.
    std::unordered_map<Iceberg::IcebergPathFromMetadata, ManifestPartitionSummary> manifest_file_partition_summaries;

    {
        /// Per source manifest: its rewritten data files with the partition index of each, since one manifest may hold several partitions.
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, std::vector<std::pair<Iceberg::IcebergPathFromMetadata, size_t>>>
            grouped_by_manifest_files_result;

        std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<DataFilePlan>> patched_path_to_data_file;
        for (const auto & [_, data_file] : plan.path_to_data_file)
            patched_path_to_data_file[data_file->patched_path] = data_file;

        for (size_t i = 0; i < plan.partitions.size(); ++i)
            for (const auto & data_file : plan.partitions[i])
                grouped_by_manifest_files_result[data_file->manifest_list].emplace_back(data_file->patched_path, i);

        auto partition_spec_id = initial_metadata_object->getValue<Int32>(f_default_spec_id);
        auto partitions_specs = initial_metadata_object->getArray(f_partition_specs);
        Poco::JSON::Object::Ptr partititon_spec;

        for (size_t i = 0; i < partitions_specs->size(); ++i)
        {
            auto current_partition_spec = partitions_specs->getObject(static_cast<UInt32>(i));
            if (current_partition_spec->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
            {
                partititon_spec = current_partition_spec;
                break;
            }
        }

        std::vector<String> partition_columns;
        auto fields_from_partition_spec = partititon_spec->getArray(f_fields);
        for (UInt32 i = 0; i < fields_from_partition_spec->size(); ++i)
        {
            partition_columns.push_back(fields_from_partition_spec->getObject(i)->getValue<String>(f_name));
        }
        const auto partition_types
            = ChunkPartitioner(fields_from_partition_spec, current_schema->getArray(Iceberg::f_fields), context, sample_block_).getResultTypes();

        for (auto & [manifest_entry, data_files_with_partitions] : grouped_by_manifest_files_result)
        {
            manifest_entry->patched_path = plan.generator.generateManifestEntryName();
            manifest_file_renamings[manifest_entry->path] = manifest_entry->patched_path;
            /// Record before the write so a partially created object is still cleaned up.
            plan.generated_metadata_paths.push_back(manifest_entry->patched_path);
            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(path_resolver.resolve(manifest_entry->patched_path)),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            auto snapshot_id = plan.manifest_file_to_first_snapshot[manifest_entry->path];
            auto snapshot = snapshot_id_to_snapshot[snapshot_id];
            /// Fail closed rather than skip: a live manifest can have no rewritten snapshot for
            /// its first retained reference when that reference is a delete-only snapshot
            /// (tryGetAppendUpdate skips those when generating new snapshots), e.g. after the
            /// appending snapshot was expired and the manifest survives only through a later
            /// delete-only overwrite. Skipping would silently drop the manifest from the
            /// rewritten table -- and clearOldFiles would then delete its original files after
            /// commit. Throwing here aborts OPTIMIZE before any metadata is committed and
            /// before any original file is removed, leaving the table intact.
            if (!snapshot)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg compaction does not support this table history: live manifest {} is first referenced "
                    "by snapshot {} which produced no rewritten snapshot (e.g. a delete-only snapshot). "
                    "The table is left unchanged.",
                    manifest_entry->path.serialize(),
                    snapshot_id);

            std::vector<Iceberg::IcebergPathFromMetadata> data_files_vec;
            std::vector<Row> file_partition_values;
            ManifestPartitionSummary partition_summary(partition_types);
            for (const auto & [path, partition_index] : data_files_with_partitions)
            {
                data_files_vec.push_back(path);
                const auto & partition_value = plan.partition_encoder.getPartitionValue(partition_index);
                /// A file's tuple has the arity of the spec it was written with, which need not be the spec of this manifest.
                if (partition_value.size() != partition_columns.size())
                    throw Exception(
                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Iceberg manifest partition arity mismatch for data file {}: partition spec {} defines {} "
                        "columns but the file's partition tuple has {} values",
                        path.serialize(),
                        partition_spec_id,
                        partition_columns.size(),
                        partition_value.size());
                file_partition_values.push_back(partition_value);
                if (!partition_types.empty())
                    partition_summary.add(partition_value);
            }
            if (!partition_types.empty())
                manifest_file_partition_summaries.emplace(manifest_entry->patched_path, std::move(partition_summary));

            std::vector<UInt64> file_row_counts;
            std::vector<UInt64> file_byte_counts;
            /// Per-file statistics, aligned index-for-index with `data_files_vec`: a manifest can mix
            /// files from several partitions, so each output entry must carry its own bounds/counts.
            std::vector<const DataFileStatistics *> per_file_fresh_statistics;
            for (const auto & path : data_files_vec)
            {
                if (auto it = patched_path_to_data_file.find(path); it != patched_path_to_data_file.end())
                {
                    const auto & data_file = it->second;
                    file_row_counts.push_back(data_file->new_records_count);
                    file_byte_counts.push_back(data_file->new_bytes_count);
                    per_file_fresh_statistics.push_back(data_file->statistics.get());
                }
                else
                {
                    file_row_counts.push_back(0);
                    file_byte_counts.push_back(0);
                    per_file_fresh_statistics.push_back(nullptr);
                }
            }

            /// The manifest's original lineage, taken from the source manifest-list entry: the
            /// snapshot that actually added the manifest may already be expired from table
            /// metadata, in which case `plan.manifest_file_to_first_snapshot` (first *retained*
            /// snapshot referencing the manifest) would wrongly re-attribute it.
            const auto manifest_lineage = plan.manifest_file_lineage[manifest_entry->path];

            /// Sequence numbers restart from 1 in the rewritten history, so the source value must
            /// be remapped, never copied. Prefer the adding snapshot's new number; if it was
            /// expired, use the first retained snapshot referencing the manifest.
            Int64 remapped_sequence_number = 0;
            {
                Poco::JSON::Object::Ptr sequence_source = snapshot;
                if (auto it = snapshot_id_to_snapshot.find(manifest_lineage.added_snapshot_id);
                    it != snapshot_id_to_snapshot.end() && it->second)
                    sequence_source = it->second;
                if (sequence_source->has(Iceberg::f_metadata_sequence_number))
                    remapped_sequence_number = sequence_source->getValue<Int64>(Iceberg::f_metadata_sequence_number);
            }

            /// Record the manifest's actual content counts and lineage for its manifest-list
            /// entries. added_snapshot_id preserves which snapshot added the manifest, so
            /// manifest lists of later snapshots that carry it forward do not claim they added
            /// it; the sequence numbers use the remapped value, consistent with the entries
            /// written into the regenerated manifest below.
            {
                Int64 manifest_rows = 0;
                for (const auto rows : file_row_counts)
                    manifest_rows += static_cast<Int64>(rows);
                ManifestListEntryCounts counts;
                counts.files_count = static_cast<Int64>(data_files_vec.size());
                counts.rows_count = manifest_rows;
                counts.min_sequence_number = remapped_sequence_number;
                counts.added_snapshot_id = manifest_lineage.added_snapshot_id;
                counts.added_sequence_number = remapped_sequence_number;
                manifest_file_counts[manifest_entry->patched_path] = counts;
            }
            generateManifestFile(
                metadata_object,
                partition_columns,
                file_partition_values,
                partition_types,
                data_files_vec,
                file_row_counts,
                file_byte_counts,
                /*data_file_statistics=*/std::nullopt,
                sample_block_,
                snapshot,
                write_format,
                partititon_spec,
                partition_spec_id,
                *buffer_manifest_entry,
                Iceberg::FileContentType::DATA,
                /// Stamp the rewritten entries with the manifest's original adding snapshot and
                /// the remapped sequence number, keeping the manifest file consistent with the
                /// manifest-list entries written above and with the renumbered history.
                /* user_defined_sequence_number */ remapped_sequence_number,
                /* user_defined_snapshot_id */ manifest_lineage.added_snapshot_id,
                /*data_file_formats=*/{},
                /*per_file_statistics=*/{},
                /*data_file_sort_order_ids=*/{},
                /*per_file_entry_lineage=*/{},
                /*schema_to_serialize=*/nullptr,
                &per_file_fresh_statistics);

            buffer_manifest_entry->finalize();
            auto manifest_bytes = buffer_manifest_entry->count();
            if (manifest_bytes == 0)
            {
                auto file_metadata = object_storage->getObjectMetadata(
                    path_resolver.resolve(manifest_entry->patched_path), /*with_tags=*/ false);
                manifest_bytes = file_metadata.size_bytes;
            }
            manifest_file_sizes[manifest_entry->patched_path] += manifest_bytes;
        }
    }

    std::unordered_map<Iceberg::IcebergPathFromMetadata, Iceberg::IcebergPathFromMetadata> manifest_list_renamings;
    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        if (auto append = tryGetAppendUpdate(plan.history[i]); !append)
            continue;

        manifest_list_renamings[plan.history[i].manifest_list_path] = new_snapshots[i].manifest_list_path;
    }

    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        if (auto append = tryGetAppendUpdate(plan.history[i]); !append)
            continue;

        auto initial_manifest_list_name = plan.history[i].manifest_list_path;
        auto initial_manifest_entries = plan.manifest_list_to_manifest_files[initial_manifest_list_name];
        auto renamed_manifest_list = manifest_list_renamings[initial_manifest_list_name];
        std::vector<Iceberg::IcebergPathFromMetadata> renamed_manifest_entries;
        for (const auto & initial_manifest_entry : initial_manifest_entries)
        {
            auto renamed_manifest_entry = manifest_file_renamings[initial_manifest_entry];
            if (!renamed_manifest_entry.empty())
            {
                renamed_manifest_entries.push_back(renamed_manifest_entry);
            }
        }
        std::vector<Int64> per_manifest_sizes;
        std::vector<ManifestPartitionSummary> entry_partition_summaries;
        for (const auto & entry : renamed_manifest_entries)
        {
            per_manifest_sizes.push_back(manifest_file_sizes[entry]);
            if (auto it = manifest_file_partition_summaries.find(entry); it != manifest_file_partition_summaries.end())
                entry_partition_summaries.push_back(it->second);
        }
        if (!entry_partition_summaries.empty() && entry_partition_summaries.size() != renamed_manifest_entries.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Iceberg compaction has partition summaries for {} of {} manifests",
                entry_partition_summaries.size(),
                renamed_manifest_entries.size());
        /// Per-manifest counts are required: otherwise every entry gets the snapshot summary's
        /// `added-records` and any consumer summing them multiplies the table's row count. A
        /// manifest reports added_* only in the snapshot that added it, existing_* thereafter.
        const Int64 list_snapshot_id = new_snapshots[i].snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        std::vector<ManifestListEntryCounts> entry_counts;
        entry_counts.reserve(renamed_manifest_entries.size());
        for (const auto & entry : renamed_manifest_entries)
        {
            ManifestListEntryCounts counts;
            if (auto it = manifest_file_counts.find(entry); it != manifest_file_counts.end())
                counts = it->second;
            counts.counts_are_added = counts.added_snapshot_id.has_value() && *counts.added_snapshot_id == list_snapshot_id;
            entry_counts.push_back(counts);
        }
        plan.generated_metadata_paths.push_back(renamed_manifest_list);
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(path_resolver.resolve(renamed_manifest_list)),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());
        generateManifestList(
            path_resolver,
            metadata_object,
            object_storage,
            context,
            renamed_manifest_entries,
            new_snapshots[i].snapshot,
            per_manifest_sizes,
            *buffer_manifest_list,
            Iceberg::FileContentType::DATA,
            false,
            /* per_entry_content_types */ {},
            entry_counts,
            /* carry_forward_manifest_paths */ {},
            /* entry_partition_spec_ids */ {},
            entry_partition_summaries);
        buffer_manifest_list->finalize();
    }

    {
        std::string json_representation = stringifyJSON(metadata_object, 4);

        auto version_hint_path = plan.generator.generateVersionHint();
        /// Sampled before the write so a target that already exists can be told apart from one this
        /// rewrite may itself have created: `writeMetadataFile` reports the two the same way.
        StoredObject commit_target(path_resolver.resolve(generated_metadata_info.path));
        const bool target_existed_before = object_storage->exists(commit_target);

        /// A throw means the commit could not be attempted at all (e.g. the backend cannot express the
        /// compare-and-swap), so nothing was published and the caller removes the rewritten files.
        const bool written = Iceberg::writeMetadataFile(
            path_resolver,
            generated_metadata_info,
            json_representation,
            object_storage,
            context);

        if (!written)
        {
            /// Someone else already held the target, so nothing of ours was published.
            if (target_existed_before)
                return CommitResult::Lost;

            /// Otherwise `writeMetadataFile` also folds in an exception while writing, which can happen after
            /// the storage accepted the object, and a jointly racing writer may have taken the name
            /// since the probe above. Removing the rewritten files is safe only when the object is
            /// absent, or present holding somebody else's bytes rather than the ones written here.
            try
            {
                if (!object_storage->exists(commit_target))
                    return CommitResult::Lost;
                /// Only comparable when the metadata is stored verbatim; a compressed object holds
                /// different bytes, so there the ambiguity has to stand.
                if (generated_metadata_info.compression_method == CompressionMethod::None)
                {
                    auto published = object_storage
                                         ->readSmallObjectAndGetObjectMetadata(
                                             commit_target, context->getReadSettings(), json_representation.size() + 1)
                                         .data;
                    if (published != json_representation)
                        return CommitResult::Lost;
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to check whether the compacted metadata was written");
            }
            LOG_WARNING(
                log,
                "Iceberg compaction could not confirm whether metadata version {} was written; keeping every file so "
                "nothing referenced by it is removed",
                generated_metadata_info.version);
            return CommitResult::KeepEverything;
        }

        plan.metadata_published = true;

        /// Published from here on, so nothing below may report `Lost`. The hint is created if missing and
        /// must resolve to exactly this metadata file; otherwise a reader may still resolve the previous
        /// generation, so both are kept. A failed update counts as unconfirmed.
        bool hint_confirmed = false;
        try
        {
            hint_confirmed = Iceberg::tryWriteVersionHintFile(
                path_resolver,
                generated_metadata_info,
                version_hint_path,
                object_storage,
                context,
                /* create */ true,
                /* assert_version_exactly */ true);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update version-hint.text after Iceberg compaction");
            return CommitResult::KeepEverything;
        }

        if (!hint_confirmed)
        {
            LOG_WARNING(
                log,
                "Iceberg compaction published metadata version {} but version-hint.text was not confirmed to name it; "
                "keeping both the pre-compaction and the rewritten files so the table stays readable either way",
                generated_metadata_info.version);
            return CommitResult::KeepEverything;
        }
        return CommitResult::Published;
    }
}

static std::vector<String> getOldFiles(ObjectStoragePtr object_storage, const String & table_path)
{
    auto metadata_files = listFiles(*object_storage, table_path, "metadata", "");
    /// `version-hint.text` is a fixed path that the commit below rewrites in place rather than
    /// versioning, so deleting the pre-compaction listing would remove the hint the commit just
    /// wrote and leave `iceberg_use_version_hint = 1` readers unable to discover any metadata.
    std::erase_if(metadata_files, [](const String & file)
    {
        return file.ends_with("metadata/version-hint.text");
    });
    auto data_files = listFiles(*object_storage, table_path, "data", "");

    for (auto && data_file : data_files)
        metadata_files.push_back(data_file);

    return metadata_files;
}

/// Remove the files this compaction wrote. `plan_paths` must hold only names the rewrite generated
/// itself, never the commit target; `pre_existing` guards names that were already there.
static void removeGeneratedFiles(
    ObjectStoragePtr object_storage,
    const std::vector<String> & pre_existing,
    const std::vector<String> & plan_paths)
{
    auto log = getLogger("IcebergCompaction");
    std::unordered_set<String> keep(pre_existing.begin(), pre_existing.end());
    for (const auto & path : plan_paths)
    {
        if (keep.contains(path))
            continue;
        try
        {
            LOG_DEBUG(log, "Removing orphaned Iceberg compaction file {}", path);
            object_storage->removeObjectIfExists(StoredObject(path));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to remove an orphaned Iceberg compaction file");
        }
    }
}

static void clearOldFiles(ObjectStoragePtr object_storage, const std::vector<String> & old_files)
{
    for (const auto & metadata_file : old_files)
    {
        object_storage->removeObjectIfExists(StoredObject(metadata_file));
    }
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

        auto metadata_object = deepCopy(getMetadataJSONObject(
            metadata_file_path,
            object_storage_,
            persistent_table_components.metadata_cache,
            context_,
            log,
            persistent_table_components.metadata_compression_method,
            persistent_table_components.table_uuid));

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

void compactIcebergTable(
    IcebergHistory snapshots_info,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage_,
    const DataLakeStorageSettings & data_lake_settings,
    const std::optional<FormatSettings> & format_settings_,
    ContextPtr context_,
    const String & write_format)
{
    checkIfIcebergHistorySupported(snapshots_info);

    auto plan = getPlan(
        std::move(snapshots_info),
        data_lake_settings,
        persistent_table_components,
        object_storage_,
        write_format,
        context_,
        persistent_table_components.metadata_compression_method);
    if (plan.need_optimize)
    {
        /// `OPTIMIZE` does not refresh external metadata, so the caller's storage snapshot can be on
        /// an older schema than the metadata read above, and its column names would then resolve to
        /// the wrong field ids of the schema the rewritten files are written under.
        auto current_schema_block = buildCurrentSchemaHeader(
            plan.initial_metadata_object, persistent_table_components.schema_processor);

        auto old_files = getOldFiles(object_storage_, persistent_table_components.table_path);
        /// Nothing references the rewritten files until the metadata commit publishes them, so on
        /// any failure they must be removed rather than left to accumulate a full copy of the table
        /// per attempt. Only paths this rewrite generated are considered, so files a concurrent
        /// winner created are never touched.
        auto cleanup_generated = [&]()
        {
            std::vector<String> generated;
            for (const auto & [_, data_file] : plan.path_to_data_file)
                if (!data_file->patched_path.empty())
                    generated.push_back(persistent_table_components.path_resolver.resolve(data_file->patched_path));
            for (const auto & path : plan.generated_metadata_paths)
                generated.push_back(persistent_table_components.path_resolver.resolve(path));
            removeGeneratedFiles(object_storage_, old_files, generated);
        };

        auto commit = CommitResult::Lost;
        try
        {
            writeDataFiles(
                plan,
                current_schema_block,
                object_storage_,
                persistent_table_components.path_resolver,
                persistent_table_components.schema_processor,
                format_settings_,
                context_,
                write_format,
                persistent_table_components.metadata_compression_method);
            commit = writeMetadataFiles(
                plan,
                persistent_table_components.path_resolver,
                object_storage_,
                context_,
                current_schema_block,
                write_format);
        }
        catch (...)
        {
            /// Only safe because nothing was published: `plan.metadata_published` is set the moment
            /// the commit succeeds, and removing the rewritten files after that would strand the
            /// published metadata pointing at them.
            if (!plan.metadata_published)
                cleanup_generated();
            throw;
        }

        if (commit == CommitResult::Lost)
        {
            /// The table still references the pre-compaction files, so only the rewritten ones go.
            cleanup_generated();
            throw Exception(
                ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED,
                "Iceberg compaction (OPTIMIZE) lost the metadata commit to a concurrent writer; "
                "the table was modified during compaction. Retry OPTIMIZE.");
        }

        /// A reader may resolve either generation, so neither may be deleted; a later OPTIMIZE
        /// cleans up once the outcome is unambiguous.
        if (commit == CommitResult::KeepEverything)
            return;

        clearOldFiles(object_storage_, old_files);
    }
}

}

#endif
