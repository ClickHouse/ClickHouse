#include <string>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Interpreters/Cache/FileSegment.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Constant.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h"
#include "Storages/ObjectStorage/DataLakes/Common.h"
#include "Storages/ObjectStorage/StorageObjectStorageSource.h"
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include "Common/Logger.h"
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <IO/CompressionMethod.h>
#include <fmt/format.h>
#include <Poco/JSON/Object.h>

namespace Iceberg
{

using namespace DB;

struct ManifestFilePlan
{
    String path;
    std::vector<String> manifest_lists_path;

    FileNamesGenerator::Result patched_path;
};

struct DataFilePlan
{
    ParsedDataFileInfo parsed_data_file_info;
    std::shared_ptr<ManifestFilePlan> manifest_list;
    
    FileNamesGenerator::Result patched_path;
};

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
};

struct Plan
{
    using Partition = std::vector<DataFilePlan>;
    std::vector<Partition> partitions;
    IcebergMetadata::IcebergHistory history;
    std::unordered_map<String, Int64> manifest_file_to_first_snapshot;
    std::unordered_map<String, std::vector<String>> manifest_list_to_manifest_files;

    ParititonEncoder partition_encoder;    
};

Plan getPlan(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    FileNamesGenerator & generator)
{
    auto metadata = IcebergMetadata::create(object_storage, configuration, context);
    std::unique_ptr<IcebergMetadata> iceberg_metadata(static_cast<IcebergMetadata*>(metadata.release()));
    auto snapshots_info = iceberg_metadata->getHistory(context);

    Plan plan;

    std::vector<ManifestFileEntry> all_positional_delete_files;
    std::unordered_map<String, std::shared_ptr<ManifestFilePlan>> manifest_files;
    for (const auto & snapshot : snapshots_info)
    {
        auto manifest_list = iceberg_metadata->getManifestList(context, snapshot.manifest_list_path);
        for (const auto & manifest_file : manifest_list)
        {
            plan.manifest_list_to_manifest_files[snapshot.manifest_list_path].push_back(manifest_file.manifest_file_path);
            plan.manifest_file_to_first_snapshot[manifest_file.manifest_file_path] = snapshot.snapshot_id;
            auto manifest_file_content = iceberg_metadata->tryGetManifestFile(context, manifest_file.manifest_file_path, manifest_file.added_sequence_number);

            if (!manifest_files.contains(manifest_file.manifest_file_path))
            {
                manifest_files[manifest_file.manifest_file_path] = std::make_shared<ManifestFilePlan>();
                manifest_files[manifest_file.manifest_file_path]->path = manifest_file.manifest_file_path;
            }
            manifest_files[manifest_file.manifest_file_path]->manifest_lists_path.push_back(snapshot.manifest_list_path);
            auto data_files = manifest_file_content->getFiles(FileContentType::DATA);
            auto positional_delete_files = manifest_file_content->getFiles(FileContentType::POSITIONAL_DELETE);
            for (const auto & pos_delete_file : positional_delete_files)
                all_positional_delete_files.push_back(pos_delete_file);
            
            for (const auto & data_file : data_files)
            {
                auto partition_index = plan.partition_encoder.encodePartition(data_file.partition_key_value);
                if (plan.partitions.size() <= partition_index)
                    plan.partitions.push_back({});

                ParsedDataFileInfo parsed_data_file_info(configuration, data_file, {});
                plan.partitions[partition_index].push_back(DataFilePlan{
                    .parsed_data_file_info = parsed_data_file_info,
                    .manifest_list = manifest_files[manifest_file.manifest_file_path],
                    .patched_path = generator.generateDataFileName()
                });
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
            if (data_file.parsed_data_file_info.sequence_number <= delete_file.added_sequence_number)
                data_file.parsed_data_file_info.position_deletes_objects.push_back(delete_file);
        }
    }
    plan.history = std::move(snapshots_info);
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
    for (const auto & partition : initial_plan.partitions)
    {
        for (const auto & data_file : partition)
        {
            auto delete_file_transform = std::make_shared<IcebergBitmapPositionDeleteTransform>(
                sample_block,
                std::make_shared<IcebergDataObjectInfo>(std::nullopt, data_file.parsed_data_file_info),
                object_storage,
                format_settings,
                context,
                configuration->format,
                configuration->compression_method
            );

            StorageObjectStorage::ObjectInfo object_info(data_file.parsed_data_file_info.data_object_file_path);
            auto read_buffer = StorageObjectStorageSource::createReadBuffer(
                object_info,
                object_storage,
                context,
                getLogger("IcebergCompaction")
            );

            const Settings & settings = context->getSettingsRef();
            auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(
                settings,
                /*num_streams_=*/1);

            auto input_format = FormatFactory::instance().getInput(
                configuration->format,
                *read_buffer,
                *sample_block,
                context,
                8192,
                format_settings,
                parser_shared_resources,
                std::make_shared<FormatFilterInfo>(nullptr, context, nullptr),
                true /* is_remote_fs */,
                chooseCompressionMethod(data_file.parsed_data_file_info.data_object_file_path, configuration->compression_method),
                false);

                
            auto write_buffer = object_storage->writeObject(
                StoredObject(data_file.patched_path.path_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            auto output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
                configuration->format, *write_buffer, *sample_block, context, format_settings);
            
            while (true)
            {
                auto chunk = input_format->read();
                if (chunk.empty())
                    break;

                delete_file_transform->transform(chunk);
                ColumnsWithTypeAndName columns_with_types_and_name;
                for (size_t i = 0; i < sample_block->columns(); ++i)
                {
                    ColumnWithTypeAndName column(chunk.getColumns()[i], sample_block->getDataTypes()[i], sample_block->getNames()[i]);
                    columns_with_types_and_name.push_back(std::move(column));      
                }
                
                output_format->write(Block(columns_with_types_and_name));
            }
        }
    }
}

String writeMetadataFiles(
    Plan & plan,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    SharedHeader sample_block_,
    FileNamesGenerator & generator)
{
    auto log = getLogger("IcebergCompaction");
    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

    ColumnsDescription columns_description = ColumnsDescription::fromNamesAndTypes(sample_block_->getNamesAndTypes());
    auto [metadata_object, metadata_object_str] = createEmptyMetadataFile(configuration->getRawPath().path, columns_description, nullptr);

    MetadataGenerator metadata_generator(metadata_object);
    std::vector<MetadataGenerator::NextMetadataResult> new_snapshots;
    auto generated_metadata_name = generator.generateMetadataName();
    std::unordered_map<Int64, Poco::JSON::Object::Ptr> snapshot_id_to_snapshot;
    for (const auto & history_record : plan.history)
    {
        auto new_snapshot = metadata_generator.generateNextMetadata(
            generator,
            generated_metadata_name.path_in_metadata,
            history_record.parent_id,
            history_record.added_files,
            history_record.added_records,
            history_record.added_files_size,
            history_record.num_partitions);

        new_snapshots.push_back(new_snapshot);
        snapshot_id_to_snapshot[history_record.snapshot_id] = new_snapshot.snapshot;
    }
    Poco::JSON::Object::Ptr initial_metadata_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration, nullptr, context, log, compression_method);
    std::unordered_map<String, String> manifest_file_renamings;
    std::unordered_map<String, Int64> manifest_file_sizes;

    {
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, std::vector<String>> grouped_by_manifest_files_result;
        std::unordered_map<std::shared_ptr<ManifestFilePlan>, size_t> partition_values;

        for (size_t i = 0; i < plan.partitions.size(); ++i)
        {
            const auto & partition = plan.partitions[i];
            for (const auto & data_file : partition)
            {
                grouped_by_manifest_files_result[data_file.manifest_list].push_back(data_file.patched_path.path_in_metadata);   
                partition_values[data_file.manifest_list] = i;
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

        for (auto & [manifest_entry, data_filenames] : grouped_by_manifest_files_result)
        {
            manifest_entry->patched_path = generator.generateManifestEntryName();
            manifest_file_renamings[manifest_entry->path] = manifest_entry->patched_path.path_in_metadata;
            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(manifest_entry->patched_path.path_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            auto snapshot_id = plan.manifest_file_to_first_snapshot[manifest_entry->path];
            auto snapshot = snapshot_id_to_snapshot[snapshot_id];

            /// TODO: ADD PARTITIONS
            generateManifestFile(metadata_object, {}, {}, data_filenames, snapshot, configuration->format, partititon_spec, partition_spec_id, *buffer_manifest_entry);

            manifest_file_sizes[manifest_entry->patched_path.path_in_metadata] += buffer_manifest_entry->count();
            buffer_manifest_entry->finalize();
        }
    }

    std::unordered_map<String, String> manifest_list_renamings;
    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        manifest_list_renamings[plan.history[i].manifest_list_path] = new_snapshots[i].metadata_path;
    }

    for (size_t i = 0; i < plan.history.size(); ++i)
    {
        auto initial_manifest_list_name = plan.history[i].manifest_list_path;
        auto initial_manifest_entries = plan.manifest_list_to_manifest_files[initial_manifest_list_name];
        auto renamed_manifest_list = manifest_list_renamings[initial_manifest_list_name];
        std::vector<String> renamed_manifest_entries;
        Int32 total_manifest_file_sizes = 0;
        for (const auto & initial_manifest_entry : initial_manifest_entries)
        {
            auto renamed_manifest_entry = manifest_file_renamings[initial_manifest_entry];
            renamed_manifest_entries.push_back(renamed_manifest_entry);
            total_manifest_file_sizes += manifest_file_sizes[renamed_manifest_entry];
        }
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(renamed_manifest_list), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        generateManifestList(generator, metadata_object, object_storage, context, renamed_manifest_entries, new_snapshots[i].snapshot, total_manifest_file_sizes, *buffer_manifest_list, false);
        buffer_manifest_list->finalize();
    }

    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata_object, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto buffer_metadata = object_storage->writeObject(
            StoredObject(generated_metadata_name.path_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        buffer_metadata->write(json_representation.data(), json_representation.size());
        buffer_metadata->finalize();
    }
    return generated_metadata_name.path_in_storage;
}

void clearOldFiles(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const String & new_metadata_path,
    const Plan & plan)
{
    const auto metadata_files = listFiles(*object_storage, *configuration, "metadata", ".metadata.json");
    for (const auto & metadata_file : metadata_files)
    {
        if (metadata_file == new_metadata_path)
            continue;
        object_storage->removeObjectIfExists(StoredObject(metadata_file));
    }

    for (const auto & partition : plan.partitions)
    {
        for (const auto & data_file : partition)
        {
            object_storage->removeObjectIfExists(StoredObject(data_file.parsed_data_file_info.data_object_file_path));   
            for (const auto & manifest_list : data_file.manifest_list->manifest_lists_path)
                object_storage->removeObjectIfExists(StoredObject(manifest_list));
            object_storage->removeObjectIfExists(StoredObject(data_file.manifest_list->path));
        }
    }
}

void compactIcebergTable(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
{
    FileNamesGenerator generator(configuration_->getRawPath().path, configuration_->getRawPath().path, false);
    auto plan = getPlan(object_storage_, configuration_, context_, generator);
    writeDataFiles(plan, sample_block_, object_storage_, format_settings_, context_, configuration_);
    auto metadata_file = writeMetadataFiles(plan, object_storage_, configuration_, context_, sample_block_, generator);
    clearOldFiles(object_storage_, configuration_, metadata_file, plan);
}

}
