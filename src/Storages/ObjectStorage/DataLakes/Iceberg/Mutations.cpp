#include <Columns/ColumnString.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <IO/CompressionMethod.h>
#include <Processors/Chunk.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Storages/AlterCommands.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB::DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace DB::FailPoints
{
extern const char iceberg_writes_cleanup[];
}

namespace DB::Iceberg
{

#if USE_AVRO

static constexpr const char * block_datafile_path = "_path";
static constexpr const char * block_row_number = "_row_number";

struct DeleteFileWriteResult
{
    FileNamesGenerator::Result path;
    Int32 total_rows;
    Int32 total_bytes;
};

using DeleteFileWriteResultByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DeleteFileWriteResult, ChunkPartitioner::PartitionKeyHasher>;
using DeleteFileStatisticsByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DataFileStatistics, ChunkPartitioner::PartitionKeyHasher>;

struct DeleteFileWriteResultWithStats
{
    DeleteFileWriteResultByPartitionKey data_file;
    DeleteFileStatisticsByPartitionKey statistic;
};

static Block getPositionDeleteFileSampleBlock()
{
    ColumnsWithTypeAndName delete_file_columns_desc;
    delete_file_columns_desc.push_back(
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), IcebergPositionDeleteTransform::data_file_path_column_name));
    delete_file_columns_desc.push_back(
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), IcebergPositionDeleteTransform::positions_column_name));

    return Block(delete_file_columns_desc);
}

std::optional<DeleteFileWriteResultWithStats> writeDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    FileNamesGenerator & generator,
    const std::optional<FormatSettings> & format_settings,
    ChunkPartitioner & chunk_partitioner)
{
    chassert(commands.size() == 1);

    auto storage_ptr = DatabaseCatalog::instance().getTable(storage_id, context);
    DeleteFileWriteResultByPartitionKey result;
    DeleteFileStatisticsByPartitionKey statistics;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> writers;

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        Block block;
        bool has_any_rows = false;
        while (executor.pull(block))
        {
            has_any_rows = true;
            Chunk chunk(block.getColumns(), block.rows());
            auto partition_result = chunk_partitioner.partitionChunk(chunk);

            auto col_data_filename = block.getByName(block_datafile_path);
            auto col_position = block.getByName(block_row_number);

            size_t col_data_filename_index = 0;
            size_t col_position_index = 0;
            for (size_t i = 0; i < block.columns(); ++i)
            {
                if (block.getNames()[i] == block_datafile_path)
                    col_data_filename_index = i;
                if (block.getNames()[i] == block_row_number)
                    col_position_index = i;
            }

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                if (!statistics.contains(partition_key))
                    statistics.emplace(partition_key, DataFileStatistics(IcebergPositionDeleteTransform::getSchemaFields()));

                if (!writers.contains(partition_key))
                {
                    auto delete_file_info = generator.generatePositionDeleteFile();

                    result[partition_key].path = delete_file_info;
                    auto write_buffer = object_storage->writeObject(
                        StoredObject(delete_file_info.path_in_storage),
                        WriteMode::Rewrite,
                        std::nullopt,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        context->getWriteSettings());

                    auto delete_file_sample_block = getPositionDeleteFileSampleBlock();
                    ColumnMapperPtr column_mapper = std::make_shared<ColumnMapper>();
                    std::unordered_map<String, Int64> field_ids;
                    field_ids[IcebergPositionDeleteTransform::positions_column_name] = IcebergPositionDeleteTransform::positions_column_field_id;
                    field_ids[IcebergPositionDeleteTransform::data_file_path_column_name] = IcebergPositionDeleteTransform::data_file_path_column_field_id;
                    column_mapper->setStorageColumnEncoding(std::move(field_ids));
                    FormatFilterInfoPtr format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper);
                    auto output_format = FormatFactory::instance().getOutputFormat(
                        configuration->format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

                    write_buffers[partition_key] = std::move(write_buffer);
                    writers[partition_key] = std::move(output_format);
                }

                col_data_filename.column = partition_chunk.getColumns()[col_data_filename_index];
                col_position.column = partition_chunk.getColumns()[col_position_index];

                auto col_data_filename_without_namespaces = ColumnString::create();
                for (size_t i = 0; i < col_data_filename.column->size(); ++i)
                {
                    Field cur_value;
                    col_data_filename.column->get(i, cur_value);

                    String path_without_namespace;
                    if (cur_value.safeGet<String>().starts_with(configuration->getNamespace()))
                        path_without_namespace = cur_value.safeGet<String>().substr(configuration->getNamespace().size());

                    if (!path_without_namespace.starts_with(configuration->getPathForRead().path))
                    {
                        if (path_without_namespace.starts_with('/'))
                            path_without_namespace = path_without_namespace.substr(1);
                        else
                            path_without_namespace = "/" + path_without_namespace;
                    }
                    col_data_filename_without_namespaces->insert(path_without_namespace);
                }
                col_data_filename.column = std::move(col_data_filename_without_namespaces);
                Columns chunk_pos_delete;
                chunk_pos_delete.push_back(col_data_filename.column);
                chunk_pos_delete.push_back(col_position.column);
                auto stats_chunk = Chunk(chunk_pos_delete, partition_chunk.getNumRows());
                statistics.at(partition_key).update(stats_chunk);

                Block delete_file_block({col_data_filename, col_position});
                result[partition_key].total_rows += delete_file_block.rows();
                writers[partition_key]->write(delete_file_block);
            }
        }

        if (!has_any_rows)
            return std::nullopt;

        for (const auto & [partition_key, _] : result)
        {
            writers[partition_key]->flush();
            writers[partition_key]->finalize();
            write_buffers[partition_key]->finalize();
            result[partition_key].total_bytes = static_cast<Int32>(write_buffers[partition_key]->count());
        }
        return DeleteFileWriteResultWithStats{result, statistics};
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg supports only delete mutations");
}

bool writeMetadataFiles(
    DeleteFileWriteResultWithStats & delete_filenames,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id,
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    ChunkPartitioner & chunk_partitioner)
{
    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    Int32 total_rows = 0;
    Int32 total_bytes = 0;
    for (const auto & [_, delete_filename] : delete_filenames.data_file)
    {
        total_rows += delete_filename.total_rows;
        total_bytes += delete_filename.total_bytes;
    }

    auto [new_snapshot, manifest_list_name, storage_manifest_list_name]
        = MetadataGenerator(metadata).generateNextMetadata(filename_generator, metadata_name, parent_snapshot, /* added_files */0, /* added_records */0, total_bytes, /* num_partitions */1, /* added_delete_files */1, total_rows);

    Strings manifest_entries_in_storage;
    Strings manifest_entries;
    Int32 manifest_lengths = 0;

    auto cleanup = [&]()
    {
        try
        {
            for (const auto & [_, data_file] : delete_filenames.data_file)
                object_storage->removeObjectIfExists(StoredObject(data_file.path.path_in_storage));

            for (const auto & manifest_filename_in_storage : manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
            object_storage->removeObjectIfExists(StoredObject(storage_metadata_name));
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("IcebergMutations"), "Iceberg cleanup failed");
        }
    };

    try
    {
        for (const auto & [partition_key, delete_filename] : delete_filenames.data_file)
        {
            auto [manifest_entry_name, storage_manifest_entry_name] = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage.push_back(storage_manifest_entry_name);
            manifest_entries.push_back(manifest_entry_name);

            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(storage_manifest_entry_name),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());
            try
            {
                generateManifestFile(
                    metadata,
                    chunk_partitioner.getColumns(),
                    partition_key,
                    chunk_partitioner.getResultTypes(),
                    {delete_filename.path.path_in_metadata},
                    delete_filenames.statistic.at(partition_key),
                    std::make_shared<const Block>(getPositionDeleteFileSampleBlock()),
                    new_snapshot,
                    configuration->format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    Iceberg::FileContentType::POSITION_DELETE);
                buffer_manifest_entry->finalize();
                manifest_lengths += buffer_manifest_entry->count();
            }
            catch (...)
            {
                cleanup();
                throw;
            }
        }

        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            try
            {
                generateManifestList(
                    filename_generator,
                    metadata,
                    object_storage,
                    context,
                    manifest_entries,
                    new_snapshot,
                    manifest_lengths,
                    *buffer_manifest_list,
                    Iceberg::FileContentType::POSITION_DELETE);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup();
                throw;
            }
        }

        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            if (object_storage->exists(StoredObject(storage_metadata_name)))
            {
                cleanup();
                return false;
            }

            Iceberg::writeMessageToFile(json_representation, storage_metadata_name, object_storage, context, cleanup);
            if (configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
            {
                auto filename_version_hint = filename_generator.generateVersionHint();
                Iceberg::writeMessageToFile(storage_metadata_name, filename_version_hint.path_in_storage, object_storage, context, cleanup);
            }
            if (catalog)
            {
                String catalog_filename = metadata_name;
                if (!catalog_filename.starts_with(configuration->getTypeName()))
                    catalog_filename = configuration->getTypeName() + "://" + configuration->getNamespace() + "/" + metadata_name;

                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
                {
                    cleanup();
                    object_storage->removeObjectIfExists(StoredObject(storage_metadata_name));
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
    return true;
}

void mutate(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr storage_metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    FileNamesGenerator filename_generator(configuration->getRawPath().path, configuration->getRawPath().path, false, CompressionMethod::None);

    auto log = getLogger("IcebergMutations");
    auto [last_version, metadata_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

    filename_generator.setVersion(last_version + 1);
    filename_generator.setCompressionMethod(compression_method);

    auto metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);
    auto partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);
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

    auto current_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(static_cast<UInt32>(i));
        }
    }

    const auto sample_block = std::make_shared<const Block>(storage_metadata->getSampleBlock());
    auto chunk_partitioner = ChunkPartitioner(partititon_spec->getArray(Iceberg::f_fields), current_schema, context, sample_block);
    auto delete_file = writeDataFiles(commands, context, storage_metadata, storage_id, object_storage, configuration, filename_generator, format_settings, chunk_partitioner);

    if (delete_file)
    {
        while (!writeMetadataFiles(*delete_file, object_storage, configuration, context, filename_generator, catalog, storage_id, metadata, partititon_spec, partition_spec_id, chunk_partitioner))
        {
        }
    }
}

void alter(
    const AlterCommands & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    FileNamesGenerator filename_generator(configuration->getRawPath().path, configuration->getRawPath().path, false, CompressionMethod::None);
    auto log = getLogger("IcebergMutations");
    auto [last_version, metadata_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

    filename_generator.setVersion(last_version + 1);
    filename_generator.setCompressionMethod(compression_method);

    auto metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);

    auto metadata_json_generator = MetadataGenerator(metadata);

    switch (params[0].type)
    {
        case AlterCommand::Type::ADD_COLUMN:
            metadata_json_generator.generateAddColumnMetadata(params[0].column_name, params[0].data_type);
            break;
        case AlterCommand::Type::DROP_COLUMN:
            metadata_json_generator.generateDropColumnMetadata(params[0].column_name);
            break;
        case AlterCommand::Type::MODIFY_COLUMN:
            metadata_json_generator.generateModifyColumnMetadata(params[0].column_name, params[0].data_type);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type of alter {}", params[0].type);
    }

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(metadata, oss, 4);
    std::string json_representation = removeEscapedSlashes(oss.str());

    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();
    auto cleanup = [] {};
    Iceberg::writeMessageToFile(json_representation, storage_metadata_name, object_storage, context, cleanup);

    if (configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
    {
        auto filename_version_hint = filename_generator.generateVersionHint();
        Iceberg::writeMessageToFile(storage_metadata_name, filename_version_hint.path_in_storage, object_storage, context, cleanup);
    }
}

#endif

}
