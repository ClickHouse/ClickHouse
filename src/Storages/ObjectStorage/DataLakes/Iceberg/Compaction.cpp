#include <limits>
#include <optional>
#include <string>
#include <unordered_set>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <Databases/DataLake/Common.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
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
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorDump.h>
#include <Common/Logger.h>

#if USE_AVRO && !CLICKHOUSE_CLOUD

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int NOT_IMPLEMENTED;
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

struct ManifestFilePlan
{
    explicit ManifestFilePlan(Poco::JSON::Array::Ptr schema_)
        : statistics(schema_)
    {
    }

    Iceberg::IcebergPathFromMetadata path;
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_lists_path;
    DataFileStatistics statistics;

    Iceberg::IcebergPathFromMetadata patched_path;
};

struct DataFilePlan
{
    IcebergDataObjectInfoPtr data_object_info;
    std::shared_ptr<ManifestFilePlan> manifest_list;

    Iceberg::IcebergPathFromMetadata patched_path;
    UInt64 new_records_count = 0;
    UInt64 new_bytes_count = 0;
};

/// Compaction plan: all data files, the delete files applied to them, and prior metadata.
struct Plan
{
    bool need_optimize = false;
    using PartitionPlan = std::vector<std::shared_ptr<DataFilePlan>>;
    std::vector<PartitionPlan> partitions;
    IcebergHistory history;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Int64> manifest_file_to_first_snapshot;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::vector<Iceberg::IcebergPathFromMetadata>> manifest_list_to_manifest_files;
    std::unordered_map<Int64, std::vector<std::shared_ptr<DataFilePlan>>> snapshot_id_to_data_files;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<DataFilePlan>> path_to_data_file;
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
        persistent_table_components.metadata_compression_method);

    Poco::JSON::Object::Ptr initial_metadata_object
        = getMetadataJSONObject(metadata_file_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid);

    if (initial_metadata_object->getValue<Int32>(Iceberg::f_format_version) < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Compaction is supported only for format_version 2.");

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

    std::vector<ProcessedManifestFileEntryPtr> all_positional_delete_files;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<ManifestFilePlan>> manifest_files;
    for (const auto & snapshot : snapshots_info)
    {
        auto manifest_list = getManifestList(object_storage, persistent_table_components, context, snapshot.manifest_list_path, log);
        for (const auto & manifest_file : manifest_list)
        {
            plan.manifest_list_to_manifest_files[snapshot.manifest_list_path].push_back(manifest_file.manifest_file_path);
            if (!plan.manifest_file_to_first_snapshot.contains(manifest_file.manifest_file_path))
                plan.manifest_file_to_first_snapshot[manifest_file.manifest_file_path] = snapshot.snapshot_id;
            auto files_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log, manifest_file, static_cast<Int32>(current_schema_id));

            if (!manifest_files.contains(manifest_file.manifest_file_path))
            {
                manifest_files[manifest_file.manifest_file_path] = std::make_shared<ManifestFilePlan>(current_schema);
                manifest_files[manifest_file.manifest_file_path]->path = manifest_file.manifest_file_path;
            }
            manifest_files[manifest_file.manifest_file_path]->manifest_lists_path.push_back(snapshot.manifest_list_path);
            for (const auto & pos_delete_file : files_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                all_positional_delete_files.push_back(pos_delete_file);

            for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
            {
                auto partition_index = plan.partition_encoder.encodePartition(data_file->parsed_entry->partition_key_value);
                if (plan.partitions.size() <= partition_index)
                    plan.partitions.push_back({});

                IcebergDataObjectInfoPtr data_object_info = std::make_shared<IcebergDataObjectInfo>(
                    data_file, persistent_table_components.path_resolver.resolve(data_file->parsed_entry->file_path_key), 0);
                std::shared_ptr<DataFilePlan> data_file_ptr;
                if (!plan.path_to_data_file.contains(manifest_file.manifest_file_path))
                {
                    data_file_ptr = std::make_shared<DataFilePlan>(DataFilePlan{
                        .data_object_info = data_object_info,
                        .manifest_list = manifest_files[manifest_file.manifest_file_path],
                        .patched_path = plan.generator.generateDataFileName()});
                    plan.path_to_data_file[manifest_file.manifest_file_path] = data_file_ptr;
                }
                else
                {
                    data_file_ptr = plan.path_to_data_file[manifest_file.manifest_file_path];
                }
                plan.partitions[partition_index].push_back(data_file_ptr);
                plan.snapshot_id_to_data_files[snapshot.snapshot_id].push_back(plan.partitions[partition_index].back());
            }
        }
    }

    for (const auto & delete_file : all_positional_delete_files)
    {
        auto partition_index = plan.partition_encoder.encodePartition(delete_file->parsed_entry->partition_key_value);
        if (partition_index >= plan.partitions.size())
            continue;

        for (auto & data_file : plan.partitions[partition_index])
        {
            if (data_file->data_object_info->info.sequence_number <= delete_file->sequence_number)
                data_file->data_object_info->addPositionDeleteObject(
                    delete_file, persistent_table_components.path_resolver.resolve(delete_file->parsed_entry->file_path_key));
        }
    }
    plan.history = std::move(snapshots_info);
    plan.need_optimize = !all_positional_delete_files.empty();
    return plan;
}

static void writeDataFiles(
    Plan & initial_plan,
    SharedHeader sample_block,
    ObjectStoragePtr object_storage,
    const IcebergPathResolver & path_resolver,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    const String & write_format,
    CompressionMethod write_compression_method)
{
    ColumnMapperPtr column_mapper;
    {
        auto current_schema_id = initial_plan.initial_metadata_object->getValue<Int64>(Iceberg::f_current_schema_id);
        auto schemas = initial_plan.initial_metadata_object->getArray(Iceberg::f_schemas);
        for (size_t i = 0; i < schemas->size(); ++i)
        {
            auto schema_object = schemas->getObject(static_cast<UInt32>(i));
            if (schema_object->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
            {
                column_mapper = createColumnMapper(schema_object);
                break;
            }
        }
    }

    for (auto & [_, data_file] : initial_plan.path_to_data_file)
    {
        auto delete_file_transform = std::make_shared<IcebergBitmapPositionDeleteTransform>(
            sample_block,
            data_file->data_object_info,
            object_storage,
            format_settings,
            // todo make compaction using same FormatParserSharedResources
            std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), 1),
            context);

        RelativePathWithMetadata relative_path(data_file->data_object_info->getPath());
        auto read_buffer = createReadBuffer(relative_path, object_storage, context, getLogger("IcebergCompaction"));

        const Settings & settings = context->getSettingsRef();
        auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(
            settings,
            /*num_streams_=*/1);

        auto input_format = FormatFactory::instance().getInput(
            data_file->data_object_info->getFileFormat().value_or(write_format),
            *read_buffer,
            *sample_block,
            context,
            8192,
            format_settings,
            parser_shared_resources,
            std::make_shared<FormatFilterInfo>(nullptr, context, nullptr, nullptr, nullptr),
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

            data_file->manifest_list->statistics.update(chunk);
            delete_file_transform->transform(chunk);
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

    /// After partition evolution each manifest must be rewritten under the spec its source files used; resolve and cache spec info per spec-id.
    struct ResolvedPartitionSpec
    {
        Poco::JSON::Object::Ptr spec;
        std::vector<String> partition_columns;
        DataTypes partition_types;
    };
    std::unordered_map<Int32, ResolvedPartitionSpec> resolved_specs;
    auto resolve_partition_spec = [&](Int32 spec_id) -> const ResolvedPartitionSpec &
    {
        if (auto it = resolved_specs.find(spec_id); it != resolved_specs.end())
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

        /// Derive partition value types from a schema that defines every source column the spec references, preferring the current schema then any historical one; register all schemas first so they can be queried by id.
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

        Int32 schema_id_for_spec = static_cast<Int32>(current_schema_id);
        std::optional<Block> spec_sample_block = build_sample_block(schema_id_for_spec);
        if (!spec_sample_block)
        {
            for (UInt32 i = 0; i < schemas->size(); ++i)
            {
                Int32 candidate_id = schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id);
                if (candidate_id == schema_id_for_spec)
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

        return resolved_specs.emplace(spec_id, std::move(resolved)).first->second;
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

    // Collect data files grouped by (partition spec-id, partition key)
    struct PartitionData
    {
        /// The partition spec the source files were written with; the rewritten manifest reuses it.
        Int32 partition_spec_id = 0;
        /// The schema the source files were written under; files of different schemas are grouped separately so each rewritten manifest's `schema` header matches all its entries.
        Int32 schema_id = 0;
        Row partition_values;
        std::vector<IcebergPathFromMetadata> file_paths;
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

        explicit PartitionData(Poco::JSON::Array::Ptr /*schema*/)
        {}
    };

    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    std::unordered_map<String, PartitionData> partitions_map;

    // Collect live data files from the current snapshot only; iterating older snapshots would resurrect deleted files.
    size_t total_data_files = 0;
    // Only data manifests are consolidated; delete-file manifests are carried forward unchanged so deleted rows do not reappear.
    size_t num_data_manifests = 0;
    std::unordered_set<String> delete_manifest_paths;

    auto current_manifest_list = getManifestList(
        object_storage, persistent_table_components, context, IcebergPathFromMetadata::deserialize(current_manifest_list_path), log);

    for (const auto & manifest_file : current_manifest_list)
    {
        if (manifest_file.content_type == ManifestFileContentType::DELETE)
        {
            delete_manifest_paths.insert(manifest_file.manifest_file_path.serialize());
            continue;
        }
        ++num_data_manifests;
        const Int32 source_partition_spec_id = manifest_file.partition_spec_id;

        /// A manifest-only rewrite cannot round-trip per-file `key_metadata` (data-file encryption keys), so reject rather than silently dropping it and making an encrypted table unreadable.
        {
            RelativePathWithMetadata key_metadata_object_info(persistent_table_components.path_resolver.resolve(manifest_file.manifest_file_path));
            auto key_metadata_buf = createReadBuffer(key_metadata_object_info, object_storage, context, log);
            AvroForIcebergDeserializer key_metadata_deserializer(std::move(key_metadata_buf), manifest_file.manifest_file_path, getFormatSettings(context));
            if (key_metadata_deserializer.hasPath(c_data_file_key_metadata))
            {
                for (size_t row = 0; row < key_metadata_deserializer.rows(); ++row)
                    if (!key_metadata_deserializer.getValueFromRowByName(row, c_data_file_key_metadata).isNull())
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "OPTIMIZE TABLE ... MANIFEST is not supported for Iceberg tables with per-file key_metadata "
                            "(encrypted data files): preserving the encryption metadata across a manifest rewrite is not implemented");
            }
        }

        auto files_handle = getManifestFileEntriesHandle(
            object_storage, persistent_table_components, context, log, manifest_file, static_cast<Int32>(current_schema_id));

        for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
        {
            // Group by source spec-id AND source schema-id so files of different specs or schemas are never merged into one manifest (a manifest carries a single spec and one `schema` header); FieldVisitorDump's type tag prevents UInt64/Int64 collisions.
            const Int32 source_schema_id = data_file->resolved_schema_id;
            String partition_key = std::to_string(source_partition_spec_id) + "|" + std::to_string(source_schema_id) + "|";
            FieldVisitorDump dump_visitor;
            for (const auto & val : data_file->parsed_entry->partition_key_value)
                partition_key += applyVisitor(dump_visitor, val) + "|";

            if (!partitions_map.contains(partition_key))
                partitions_map.emplace(partition_key, PartitionData(schema_fields));

            auto & pd = partitions_map.at(partition_key);
            pd.partition_spec_id = source_partition_spec_id;
            pd.schema_id = source_schema_id;
            pd.partition_values = data_file->parsed_entry->partition_key_value;
            // A single manifest file should not list the same data file twice
            if (std::find(pd.file_paths.begin(), pd.file_paths.end(), data_file->parsed_entry->file_path_key) == pd.file_paths.end())
            {
                pd.file_paths.push_back(data_file->parsed_entry->file_path_key);
                pd.file_metrics.emplace_back(data_file->parsed_entry->record_count, data_file->parsed_entry->file_size_in_bytes);
                pd.file_formats.push_back(data_file->parsed_entry->file_format);
                pd.file_sort_order_ids.push_back(data_file->parsed_entry->sort_order_id);

                /// Preserve the entry's lineage, resolving inherited (null) snapshot-id and sequence numbers from the manifest, since EXISTING entries require them non-null.
                DataFileEntryLineage lineage;
                lineage.added_snapshot_id = data_file->parsed_entry->parsed_snapshot_id;
                if (!lineage.added_snapshot_id.has_value())
                    lineage.added_snapshot_id = manifest_file.added_snapshot_id;
                lineage.sequence_number = data_file->parsed_entry->parsed_sequence_number;
                if (!lineage.sequence_number.has_value())
                    lineage.sequence_number = manifest_file.added_sequence_number;
                /// `file_sequence_number` is preserved separately: it can differ from the data sequence number and, when null, inherits the manifest's sequence number.
                lineage.file_sequence_number = data_file->parsed_entry->parsed_file_sequence_number;
                if (!lineage.file_sequence_number.has_value())
                    lineage.file_sequence_number = manifest_file.added_sequence_number;
                pd.file_entry_lineage.push_back(lineage);

                /// Carry the source file's per-column stats over verbatim, keeping bounds as the raw serialized bytes so they round-trip.
                DataFileColumnStatistics stats;
                for (const auto & [field_id, col_info] : data_file->parsed_entry->columns_infos)
                {
                    if (col_info.bytes_size.has_value())
                        stats.column_sizes.emplace_back(field_id, *col_info.bytes_size);
                    if (col_info.rows_count.has_value())
                        stats.value_counts.emplace_back(field_id, *col_info.rows_count);
                    if (col_info.nulls_count.has_value())
                        stats.null_value_counts.emplace_back(field_id, *col_info.nulls_count);
                }
                for (const auto & [field_id, bounds] : data_file->parsed_entry->value_bounds)
                {
                    if (!bounds.first.isNull())
                        stats.lower_bounds.emplace_back(field_id, bounds.first.safeGet<String>());
                    if (!bounds.second.isNull())
                        stats.upper_bounds.emplace_back(field_id, bounds.second.safeGet<String>());
                }
                pd.file_statistics.push_back(std::move(stats));

                ++total_data_files;
            }
        }
    }

    /// Data manifests already optimally consolidated (at most one per partition): rewriting cannot reduce the count, so report success.
    if (partitions_map.size() >= num_data_manifests)
    {
        LOG_INFO(log, "Manifests already optimally consolidated ({} data manifests, {} unique partitions); nothing to do",
                 num_data_manifests, partitions_map.size());
        return true;
    }

    const auto & path_resolver = persistent_table_components.path_resolver;

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
    std::vector<ManifestListEntryExistingCounts> existing_entry_counts;
    /// Parallel to consolidated_manifest_paths: each manifest's partition spec-id.
    std::vector<Int64> entry_partition_spec_ids;
    /// Parallel to consolidated_manifest_paths: each manifest's partition fields (value + type), used to recompute the manifest-list `partitions` summary.
    std::vector<std::vector<std::pair<Field, DataTypePtr>>> entry_partition_summaries;

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
        for (auto & [partition_key, pd] : partitions_map)
        {
            auto manifest_path = generator.generateManifestEntryName();
            auto storage_manifest_path = path_resolver.resolve(manifest_path);
            LOG_INFO(log, "Creating manifest file for partition '{}': {} ({} data files)",
                     partition_key, storage_manifest_path, pd.file_paths.size());

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

            existing_entry_counts.push_back(
                {static_cast<Int64>(pd.file_paths.size()), manifest_existing_rows, manifest_min_sequence_number});

            /// Rewrite this manifest under the partition spec its source files used, not the default.
            const auto & resolved_spec = resolve_partition_spec(pd.partition_spec_id);
            entry_partition_spec_ids.push_back(pd.partition_spec_id);

            /// All files in this manifest share one partition value, so the summary's lower/upper bounds are exactly that value.
            std::vector<std::pair<Field, DataTypePtr>> partition_summary;
            for (size_t i = 0; i < resolved_spec.partition_types.size(); ++i)
            {
                Field partition_value = i < pd.partition_values.size() ? pd.partition_values[i] : Field{};
                partition_summary.emplace_back(partition_value, resolved_spec.partition_types[i]);
            }
            entry_partition_summaries.push_back(std::move(partition_summary));

            generateManifestFile(
                metadata_object,
                resolved_spec.partition_columns,
                pd.partition_values,
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

    LOG_INFO(log, "Successfully created {} partition manifest file(s) covering {} data files",
             consolidated_manifest_paths.size(), total_data_files);
    return true;
}

namespace
{

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
            const auto & update = summary->getUpdate<Iceberg::SnapshotSummaryUpdateOverwrite>();
            /// current compaction (OPTIME TABLE my_iceberg) supports only overwrites wich has only position delete files
            if (update.added_files == 0 && (update.added_position_deletes == update.added_delete_files) && update.added_position_deletes != 0)
                return std::nullopt;
            [[fallthrough]];
        }
        case SnapshotSummaryOperation::REPLACE:
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported snapshot's operation type {}", summary->getOperation());
    }
};


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

static void writeMetadataFiles(
    Plan & plan, const IcebergPathResolver & path_resolver, ObjectStoragePtr object_storage, ContextPtr context, SharedHeader sample_block_, String write_format, String table_path)
{
    auto log = getLogger("IcebergCompaction");

    ColumnsDescription columns_description = ColumnsDescription::fromNamesAndTypes(sample_block_->getNamesAndTypes());
    auto [metadata_object, metadata_object_str] = createEmptyMetadataFile(table_path, columns_description, nullptr, nullptr, context);

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

    MetadataGenerator metadata_generator(metadata_object);
    std::vector<MetadataGenerator::NextMetadataResult> new_snapshots;
    auto generated_metadata_info = plan.generator.generateMetadataPathWithInfo();
    std::unordered_map<Int64, Poco::JSON::Object::Ptr> snapshot_id_to_snapshot;

    std::unordered_map<Int64, UInt64> snapshot_id_to_records_count;

    for (const auto & history_record : plan.history)
    {
        auto append = tryGetAppendUpdate(history_record);
        if (!append)
        {
            new_snapshots.push_back(MetadataGenerator::NextMetadataResult{});
            continue;
        }

        Int32 total_records_count = 0;
        for (const auto & data_file : plan.snapshot_id_to_data_files[history_record.snapshot_id])
            total_records_count += data_file->new_records_count;

        auto new_snapshot = metadata_generator.generateNextMetadata(
            plan.generator,
            generated_metadata_info.path,
            history_record.parent_id,
            append->added_files,
            total_records_count,
            append->added_files_size,
            append->num_partitions,
            0,
            0,
            history_record.snapshot_id,
            history_record.made_current_at.value);

        new_snapshots.push_back(new_snapshot);
        snapshot_id_to_snapshot[history_record.snapshot_id] = new_snapshot.snapshot;
    }

    Poco::JSON::Object::Ptr initial_metadata_object = plan.initial_metadata_object;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Iceberg::IcebergPathFromMetadata> manifest_file_renamings;
    std::unordered_map<Iceberg::IcebergPathFromMetadata, Int64> manifest_file_sizes;

    {
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, std::unordered_set<Iceberg::IcebergPathFromMetadata>> grouped_by_manifest_files_result;
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, size_t> grouped_by_manifest_files_partitions;

        std::unordered_map<Iceberg::IcebergPathFromMetadata, std::shared_ptr<DataFilePlan>> patched_path_to_data_file;
        for (const auto & [_, data_file] : plan.path_to_data_file)
            patched_path_to_data_file[data_file->patched_path] = data_file;

        for (size_t i = 0; i < plan.partitions.size(); ++i)
        {
            const auto & partition = plan.partitions[i];
            for (const auto & data_file : partition)
            {
                grouped_by_manifest_files_partitions[data_file->manifest_list] = i;
                grouped_by_manifest_files_result[data_file->manifest_list].insert(data_file->patched_path);
            }
        }

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

        for (auto & [manifest_entry, data_filenames] : grouped_by_manifest_files_result)
        {
            manifest_entry->patched_path = plan.generator.generateManifestEntryName();
            manifest_file_renamings[manifest_entry->path] = manifest_entry->patched_path;
            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(path_resolver.resolve(manifest_entry->patched_path)),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            auto snapshot_id = plan.manifest_file_to_first_snapshot[manifest_entry->path];
            auto snapshot = snapshot_id_to_snapshot[snapshot_id];
            if (!snapshot)
                continue;

            std::vector<Iceberg::IcebergPathFromMetadata> data_files_vec(data_filenames.begin(), data_filenames.end());
            std::vector<UInt64> file_row_counts;
            std::vector<UInt64> file_byte_counts;
            for (const auto & path : data_files_vec)
            {
                if (auto it = patched_path_to_data_file.find(path); it != patched_path_to_data_file.end())
                {
                    file_row_counts.push_back(it->second->new_records_count);
                    file_byte_counts.push_back(it->second->new_bytes_count);
                }
                else
                {
                    file_row_counts.push_back(0);
                    file_byte_counts.push_back(0);
                }
            }
            generateManifestFile(
                metadata_object,
                partition_columns,
                plan.partition_encoder.getPartitionValue(grouped_by_manifest_files_partitions[manifest_entry]),
                ChunkPartitioner(fields_from_partition_spec, current_schema->getArray(Iceberg::f_fields), context, sample_block_).getResultTypes(),
                data_files_vec,
                file_row_counts,
                file_byte_counts,
                manifest_entry->statistics,
                sample_block_,
                snapshot,
                write_format,
                partititon_spec,
                partition_spec_id,
                *buffer_manifest_entry,
                Iceberg::FileContentType::DATA);

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
        for (const auto & entry : renamed_manifest_entries)
        {
            per_manifest_sizes.push_back(manifest_file_sizes[entry]);
        }
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
            false);
        buffer_manifest_list->finalize();
    }

    {
        std::string json_representation = stringifyJSON(metadata_object, 4);

        auto buffer_metadata = object_storage->writeObject(
            StoredObject(path_resolver.resolve(generated_metadata_info.path)),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        buffer_metadata->write(json_representation.data(), json_representation.size());
        buffer_metadata->finalize();
    }
}

static std::vector<String> getOldFiles(ObjectStoragePtr object_storage, const String & table_path)
{
    auto metadata_files = listFiles(*object_storage, table_path, "metadata", "");
    auto data_files = listFiles(*object_storage, table_path, "data", "");

    for (auto && data_file : data_files)
        metadata_files.push_back(data_file);

    return metadata_files;
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

        const auto [metadata_version, metadata_file_path, _] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage_,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context_,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

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
    SharedHeader sample_block_,
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
        auto old_files = getOldFiles(object_storage_, persistent_table_components.table_path);
        writeDataFiles(
            plan,
            sample_block_,
            object_storage_,
            persistent_table_components.path_resolver,
            format_settings_,
            context_,
            write_format,
            persistent_table_components.metadata_compression_method);
        writeMetadataFiles(plan, persistent_table_components.path_resolver, object_storage_, context_, sample_block_, write_format, persistent_table_components.table_path);
        clearOldFiles(object_storage_, old_files);
    }
}

}

#endif
