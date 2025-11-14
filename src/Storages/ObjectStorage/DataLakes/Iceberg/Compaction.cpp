#include <string>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Logger.h>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

using namespace DB;

struct ManifestFilePlan
{
    explicit ManifestFilePlan(Poco::JSON::Array::Ptr schema_)
        : statistics(schema_)
    {
    }

    String path;
    std::vector<String> manifest_lists_path;
    DataFileStatistics statistics;

    FileNamesGenerator::Result patched_path;
};

struct DataFilePlan
{
    IcebergDataObjectInfoPtr data_object_info;
    std::shared_ptr<ManifestFilePlan> manifest_list;

    FileNamesGenerator::Result patched_path;
    UInt64 new_records_count = 0;
};

/// Plan of compaction consists of information about all data files and what delete files should be applied for them.
/// Also it contains some other information about previous metadata.
struct Plan
{
    bool need_optimize = false;
    using PartitionPlan = std::vector<std::shared_ptr<DataFilePlan>>;
    std::vector<PartitionPlan> partitions;
    IcebergHistory history;
    std::unordered_map<String, Int64> manifest_file_to_first_snapshot;
    std::unordered_map<String, std::vector<String>> manifest_list_to_manifest_files;
    std::unordered_map<Int64, std::vector<std::shared_ptr<DataFilePlan>>> snapshot_id_to_data_files;
    std::unordered_map<String, std::shared_ptr<DataFilePlan>> path_to_data_file;
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

Plan getPlan(
    IcebergHistory snapshots_info,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    CompressionMethod compression_method)
{
    LoggerPtr log = getLogger("IcebergCompaction::getPlan");

    Plan plan;
    plan.generator = FileNamesGenerator(configuration->getRawPath().path, configuration->getRawPath().path, false, compression_method, configuration->format);

    const auto [metadata_version, metadata_file_path, _]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

    Poco::JSON::Object::Ptr initial_metadata_object
        = getMetadataJSONObject(metadata_file_path, object_storage, configuration, nullptr, context, log, compression_method);

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

    std::vector<ManifestFileEntry> all_positional_delete_files;
    std::unordered_map<String, std::shared_ptr<ManifestFilePlan>> manifest_files;
    for (const auto & snapshot : snapshots_info)
    {
        auto manifest_list
            = getManifestList(object_storage, configuration, persistent_table_components, context, snapshot.manifest_list_path, log);
        for (const auto & manifest_file : manifest_list)
        {
            plan.manifest_list_to_manifest_files[snapshot.manifest_list_path].push_back(manifest_file.manifest_file_path);
            if (!plan.manifest_file_to_first_snapshot.contains(manifest_file.manifest_file_path))
                plan.manifest_file_to_first_snapshot[manifest_file.manifest_file_path] = snapshot.snapshot_id;
            auto manifest_file_content = getManifestFile(
                object_storage,
                configuration,
                persistent_table_components,
                context,
                log,
                manifest_file.manifest_file_path,
                manifest_file.added_sequence_number,
                manifest_file.added_snapshot_id);

            if (!manifest_files.contains(manifest_file.manifest_file_path))
            {
                manifest_files[manifest_file.manifest_file_path] = std::make_shared<ManifestFilePlan>(current_schema);
                manifest_files[manifest_file.manifest_file_path]->path = manifest_file.manifest_file_path;
            }
            manifest_files[manifest_file.manifest_file_path]->manifest_lists_path.push_back(snapshot.manifest_list_path);
            auto data_files = manifest_file_content->getFilesWithoutDeleted(FileContentType::DATA);
            auto positional_delete_files = manifest_file_content->getFilesWithoutDeleted(FileContentType::POSITION_DELETE);
            for (const auto & pos_delete_file : positional_delete_files)
                all_positional_delete_files.push_back(pos_delete_file);

            for (const auto & data_file : data_files)
            {
                auto partition_index = plan.partition_encoder.encodePartition(data_file.partition_key_value);
                if (plan.partitions.size() <= partition_index)
                    plan.partitions.push_back({});

                IcebergDataObjectInfoPtr data_object_info = std::make_shared<IcebergDataObjectInfo>(data_file, 0);
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
        auto partition_index = plan.partition_encoder.encodePartition(delete_file.partition_key_value);
        if (partition_index >= plan.partitions.size())
            continue;

        std::vector<Iceberg::ManifestFileEntry> result_delete_files;
        for (auto & data_file : plan.partitions[partition_index])
        {
            if (data_file->data_object_info->info.sequence_number <= delete_file.added_sequence_number)
                data_file->data_object_info->addPositionDeleteObject(delete_file);
        }
    }
    plan.history = std::move(snapshots_info);
    plan.need_optimize = !all_positional_delete_files.empty();
    return plan;
}

void writeDataFiles(
    Plan & initial_plan,
    SharedHeader sample_block,
    ObjectStoragePtr object_storage,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    StorageObjectStorageConfigurationPtr configuration)
{
    for (auto & [_, data_file] : initial_plan.path_to_data_file)
    {
        auto delete_file_transform = std::make_shared<IcebergBitmapPositionDeleteTransform>(
            sample_block, data_file->data_object_info, object_storage, format_settings, context);

        RelativePathWithMetadata relative_path(data_file->data_object_info->getPath());
        auto read_buffer = createReadBuffer(relative_path, object_storage, context, getLogger("IcebergCompaction"));

        const Settings & settings = context->getSettingsRef();
        auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(
            settings,
            /*num_streams_=*/1);

        auto input_format = FormatFactory::instance().getInput(
            data_file->data_object_info->getFileFormat().value_or(configuration->format),
            *read_buffer,
            *sample_block,
            context,
            8192,
            format_settings,
            parser_shared_resources,
            std::make_shared<FormatFilterInfo>(nullptr, context, nullptr, nullptr, nullptr),
            true /* is_remote_fs */,
            chooseCompressionMethod(data_file->data_object_info->getPath(), configuration->compression_method),
            false);

        auto write_buffer = object_storage->writeObject(
            StoredObject(data_file->patched_path.path_in_storage),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        auto output_format
            = FormatFactory::instance().getOutputFormat(configuration->format, *write_buffer, *sample_block, context, format_settings);

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
    }
}

void writeMetadataFiles(
    Plan & plan,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    SharedHeader sample_block_)
{
    auto log = getLogger("IcebergCompaction");

    ColumnsDescription columns_description = ColumnsDescription::fromNamesAndTypes(sample_block_->getNamesAndTypes());
    auto [metadata_object, metadata_object_str] = createEmptyMetadataFile(configuration->getRawPath().path, columns_description, nullptr);

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
    auto generated_metadata_name = plan.generator.generateMetadataName();
    std::unordered_map<Int64, Poco::JSON::Object::Ptr> snapshot_id_to_snapshot;

    std::unordered_map<Int64, UInt64> snapshot_id_to_records_count;

    for (const auto & history_record : plan.history)
    {
        if (history_record.added_files == 0)
        {
            new_snapshots.push_back(MetadataGenerator::NextMetadataResult{});
            continue;
        }
        Int32 total_records_count = 0;
        for (const auto & data_file : plan.snapshot_id_to_data_files[history_record.snapshot_id])
            total_records_count += data_file->new_records_count;

        auto new_snapshot = metadata_generator.generateNextMetadata(
            plan.generator,
            generated_metadata_name.path_in_metadata,
            history_record.parent_id,
            history_record.added_files,
            total_records_count,
            history_record.added_files_size,
            history_record.num_partitions,
            0,
            0,
            history_record.snapshot_id,
            history_record.made_current_at.value);

        new_snapshots.push_back(new_snapshot);
        snapshot_id_to_snapshot[history_record.snapshot_id] = new_snapshot.snapshot;
    }

    Poco::JSON::Object::Ptr initial_metadata_object = plan.initial_metadata_object;
    std::unordered_map<String, String> manifest_file_renamings;
    std::unordered_map<String, Int64> manifest_file_sizes;

    {
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, std::unordered_set<String>> grouped_by_manifest_files_result;
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, size_t> grouped_by_manifest_files_partitions;
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, size_t> partition_values;

        for (size_t i = 0; i < plan.partitions.size(); ++i)
        {
            const auto & partition = plan.partitions[i];
            for (const auto & data_file : partition)
            {
                grouped_by_manifest_files_partitions[data_file->manifest_list] = i;
                grouped_by_manifest_files_result[data_file->manifest_list].insert(data_file->patched_path.path_in_metadata);
                partition_values[data_file->manifest_list] = i;
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
            manifest_file_renamings[manifest_entry->path] = manifest_entry->patched_path.path_in_metadata;
            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(manifest_entry->patched_path.path_in_storage),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            auto snapshot_id = plan.manifest_file_to_first_snapshot[manifest_entry->path];
            auto snapshot = snapshot_id_to_snapshot[snapshot_id];
            if (!snapshot)
                continue;

            generateManifestFile(
                metadata_object,
                partition_columns,
                plan.partition_encoder.getPartitionValue(grouped_by_manifest_files_partitions[manifest_entry]),
                ChunkPartitioner(fields_from_partition_spec, current_schema, context, sample_block_).getResultTypes(),
                std::vector(data_filenames.begin(), data_filenames.end()),
                manifest_entry->statistics,
                sample_block_,
                snapshot,
                configuration->format,
                partititon_spec,
                partition_spec_id,
                *buffer_manifest_entry,
                Iceberg::FileContentType::DATA);

            manifest_file_sizes[manifest_entry->patched_path.path_in_metadata] += buffer_manifest_entry->count();
            buffer_manifest_entry->finalize();
        }
    }

    std::unordered_map<String, String> manifest_list_renamings;
    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        if (plan.history[i].added_files == 0)
            continue;

        manifest_list_renamings[plan.history[i].manifest_list_path] = new_snapshots[i].metadata_path;
    }

    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        if (plan.history[i].added_files == 0)
            continue;

        auto initial_manifest_list_name = plan.history[i].manifest_list_path;
        auto initial_manifest_entries = plan.manifest_list_to_manifest_files[initial_manifest_list_name];
        auto renamed_manifest_list = manifest_list_renamings[initial_manifest_list_name];
        std::vector<String> renamed_manifest_entries;
        Int32 total_manifest_file_sizes = 0;
        for (const auto & initial_manifest_entry : initial_manifest_entries)
        {
            auto renamed_manifest_entry = manifest_file_renamings[initial_manifest_entry];
            if (!renamed_manifest_entry.empty())
            {
                renamed_manifest_entries.push_back(renamed_manifest_entry);
                total_manifest_file_sizes += manifest_file_sizes[renamed_manifest_entry];
            }
        }
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(renamed_manifest_list), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        generateManifestList(
            plan.generator,
            metadata_object,
            object_storage,
            context,
            renamed_manifest_entries,
            new_snapshots[i].snapshot,
            total_manifest_file_sizes,
            *buffer_manifest_list,
            Iceberg::FileContentType::DATA,
            false);
        buffer_manifest_list->finalize();
    }

    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata_object, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto buffer_metadata = object_storage->writeObject(
            StoredObject(generated_metadata_name.path_in_storage),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        buffer_metadata->write(json_representation.data(), json_representation.size());
        buffer_metadata->finalize();
    }
}

std::vector<String> getOldFiles(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration)
{
    auto metadata_files = listFiles(*object_storage, *configuration, "metadata", "");
    auto data_files = listFiles(*object_storage, *configuration, "data", "");

    for (auto && data_file : data_files)
        metadata_files.push_back(data_file);

    return metadata_files;
}

void clearOldFiles(ObjectStoragePtr object_storage, const std::vector<String> & old_files)
{
    for (const auto & metadata_file : old_files)
    {
        object_storage->removeObjectIfExists(StoredObject(metadata_file));
    }
}

void compactIcebergTable(
    IcebergHistory snapshots_info,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    CompressionMethod compression_method_)
{
    auto plan
        = getPlan(std::move(snapshots_info), persistent_table_components, object_storage_, configuration_, context_, compression_method_);
    if (plan.need_optimize)
    {
        auto old_files = getOldFiles(object_storage_, configuration_);
        writeDataFiles(plan, sample_block_, object_storage_, format_settings_, context_, configuration_);
        writeMetadataFiles(plan, object_storage_, configuration_, context_, sample_block_);
        clearOldFiles(object_storage_, old_files);
    }
}

}

#endif
