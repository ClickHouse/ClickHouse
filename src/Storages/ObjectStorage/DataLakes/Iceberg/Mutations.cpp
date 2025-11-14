#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
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
#include <Storages/MutationCommands.h>
#include <Storages/AlterCommands.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
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
static constexpr auto MAX_TRANSACTION_RETRIES = 100;

struct DeleteFileWriteResult
{
    FileNamesGenerator::Result path;
    Int32 total_rows;
    Int32 total_bytes;
};

using DataFileWriteResultByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DeleteFileWriteResult, ChunkPartitioner::PartitionKeyHasher>;
using DataFileStatisticsByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DataFileStatistics, ChunkPartitioner::PartitionKeyHasher>;

struct DataFileWriteResultWithStats
{
    DataFileWriteResultByPartitionKey delete_file;
    DataFileStatisticsByPartitionKey delete_statistic;
};

struct WriteDataFilesResult
{
    DataFileWriteResultWithStats delete_file;
    std::optional<DataFileWriteResultWithStats> data_file;
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

static Block getNonVirtualColumns(const Block & block)
{
    auto virtual_columns_desc = VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
    std::unordered_set<String> virtual_columns;
    for (const auto & column_desc : virtual_columns_desc)
        virtual_columns.insert(column_desc);
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < block.getNames().size(); ++i)
    {
        if (virtual_columns.contains(block.getNames()[i]))
            continue;
        columns.push_back(ColumnWithTypeAndName(block.getColumns()[i], block.getDataTypes()[i], block.getNames()[i]));
    }
    return Block(columns);
}

static std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>
getPartitionedChunks(const Chunk & chunk, std::optional<ChunkPartitioner> & chunk_partitioner)
{
    if (chunk_partitioner.has_value())
        return chunk_partitioner->partitionChunk(chunk);
    auto unpartitioned_result = std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>{};
    unpartitioned_result.emplace_back(ChunkPartitioner::PartitionKey{}, chunk.clone());
    return unpartitioned_result;
}


static std::optional<WriteDataFilesResult> writeDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    FileNamesGenerator & generator,
    const std::optional<FormatSettings> & format_settings,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Poco::JSON::Object::Ptr data_schema)
{
    chassert(commands.size() == 1);

    auto storage_ptr = DatabaseCatalog::instance().getTable(storage_id, context);
    DataFileWriteResultByPartitionKey delete_data_result;
    DataFileStatisticsByPartitionKey delete_data_statistics;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> delete_data_write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> delete_data_writers;

    DataFileWriteResultByPartitionKey update_data_result;
    DataFileStatisticsByPartitionKey update_data_statistics;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> update_data_write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> update_data_writers;

    if (commands[0].type == MutationCommand::UPDATE || commands[0].type == MutationCommand::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto delete_commands = commands;
        delete_commands[0].type = MutationCommand::DELETE;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, delete_commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        Block block;
        bool has_any_rows = false;
        while (executor.pull(block))
        {
            has_any_rows = true;
            Chunk chunk(block.getColumns(), block.rows());
            auto partition_result = getPartitionedChunks(chunk, chunk_partitioner);


            size_t col_data_filename_index = block.getPositionByName(block_datafile_path);
            size_t col_position_index = block.getPositionByName(block_row_number);
            ColumnWithTypeAndName col_data_filename = block.getByPosition(col_data_filename_index);
            ColumnWithTypeAndName col_position = block.getByPosition(col_position_index);

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                if (!delete_data_statistics.contains(partition_key))
                    delete_data_statistics.emplace(partition_key, DataFileStatistics(IcebergPositionDeleteTransform::getSchemaFields()));

                if (!delete_data_writers.contains(partition_key))
                {
                    auto delete_file_info = generator.generatePositionDeleteFile();

                    delete_data_result[partition_key].path = delete_file_info;
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
                    FormatFilterInfoPtr format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
                    auto output_format = FormatFactory::instance().getOutputFormat(
                        configuration->format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

                    delete_data_write_buffers[partition_key] = std::move(write_buffer);
                    delete_data_writers[partition_key] = std::move(output_format);
                }

                col_data_filename.column = partition_chunk.getColumns()[col_data_filename_index];
                col_position.column = partition_chunk.getColumns()[col_position_index];

                if (const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(col_position.column.get()))
                {
                    const auto & null_map = nullable->getNullMapData();
                    if (std::any_of(null_map.begin(), null_map.end(), [](UInt8 x) { return x != 0; }))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null _row_number");
                    col_position.column = nullable->getNestedColumnPtr();
                }

                auto col_data_filename_without_namespaces = ColumnString::create();
                for (size_t i = 0; i < col_data_filename.column->size(); ++i)
                {
                    Field cur_value;
                    col_data_filename.column->get(i, cur_value);

                    String path_without_namespace;
                    if (cur_value.safeGet<String>().starts_with(configuration->getNamespace()))
                        path_without_namespace = cur_value.safeGet<String>().substr(configuration->getNamespace().size());

                    if (!path_without_namespace.starts_with('/'))
                        path_without_namespace = "/" + path_without_namespace;
                    col_data_filename_without_namespaces->insert(path_without_namespace);
                }
                col_data_filename.column = std::move(col_data_filename_without_namespaces);
                Columns chunk_pos_delete;
                chunk_pos_delete.push_back(col_data_filename.column);
                chunk_pos_delete.push_back(col_position.column);
                auto stats_chunk = Chunk(chunk_pos_delete, partition_chunk.getNumRows());
                delete_data_statistics.at(partition_key).update(stats_chunk);

                Block delete_file_block({col_data_filename, col_position});
                delete_data_result[partition_key].total_rows += delete_file_block.rows();
                delete_data_writers[partition_key]->write(delete_file_block);
            }
        }

        if (!has_any_rows)
            return std::nullopt;

        for (const auto & [partition_key, _] : delete_data_result)
        {
            delete_data_writers[partition_key]->flush();
            delete_data_writers[partition_key]->finalize();
            delete_data_write_buffers[partition_key]->finalize();
            delete_data_result[partition_key].total_bytes = static_cast<Int32>(delete_data_write_buffers[partition_key]->count());
        }
    }

    if (commands[0].type == MutationCommand::UPDATE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        Block block;
        while (executor.pull(block))
        {
            auto data_block = getNonVirtualColumns(block);
            Chunk chunk(data_block.getColumns(), data_block.rows());
            auto partition_result = getPartitionedChunks(chunk, chunk_partitioner);

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                if (!update_data_statistics.contains(partition_key))
                    update_data_statistics.emplace(partition_key, DataFileStatistics(data_schema->getArray(Iceberg::f_fields)));

                auto it = update_data_writers.find(partition_key);
                if (it == update_data_writers.end())
                {
                    auto data_file_info = generator.generateDataFileName();
                    update_data_result[partition_key].path = data_file_info;
                    auto data_write_buffer = object_storage->writeObject(
                        StoredObject(data_file_info.path_in_storage),
                        WriteMode::Rewrite,
                        std::nullopt,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        context->getWriteSettings());

                    auto data_output_format = FormatFactory::instance().getOutputFormat(
                        configuration->format, *data_write_buffer, data_block, context, format_settings, nullptr);

                    update_data_write_buffers[partition_key] = std::move(data_write_buffer);
                    it = update_data_writers.emplace(partition_key, std::move(data_output_format)).first;
                }

                update_data_result[partition_key].total_rows += data_block.rows();
                it->second->write(data_block);
                update_data_statistics.at(partition_key).update(chunk);
            }
        }

        for (const auto & [partition_key, _] : update_data_result)
        {
            update_data_writers[partition_key]->flush();
            update_data_writers[partition_key]->finalize();
            update_data_write_buffers[partition_key]->finalize();
            update_data_result[partition_key].total_bytes = static_cast<Int32>(update_data_write_buffers[partition_key]->count());
        }
    }

    if (commands[0].type == MutationCommand::DELETE)
        return WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, std::nullopt};
    else if (commands[0].type == MutationCommand::UPDATE)
        return WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, DataFileWriteResultWithStats{update_data_result, update_data_statistics}};
    else
        return {};
}

static bool writeMetadataFiles(
    DataFileWriteResultWithStats & delete_filenames,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id,
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Iceberg::FileContentType content_type,
    SharedHeader sample_block,
    CompressionMethod compression_method,
    bool write_metadata_json_file)
{
    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    Int32 total_rows = 0;
    Int32 total_bytes = 0;
    Int32 total_files = 0;
    for (const auto & [_, delete_filename] : delete_filenames.delete_file)
    {
        total_rows += delete_filename.total_rows;
        total_bytes += delete_filename.total_bytes;
        ++total_files;
    }

    Poco::JSON::Object::Ptr new_snapshot;
    String manifest_list_name;
    String storage_manifest_list_name;
    if (content_type == Iceberg::FileContentType::POSITION_DELETE)
    {
        auto result_generation_metadata = MetadataGenerator(metadata)
            .generateNextMetadata(
                filename_generator,
                metadata_name,
                parent_snapshot,
                /* added_files */0,
                /* added_records */0,
                total_bytes,
                /* num_partitions */total_files,
                /* added_delete_files */total_files,
                total_rows);
        new_snapshot = result_generation_metadata.snapshot;
        manifest_list_name = result_generation_metadata.metadata_path;
        storage_manifest_list_name = result_generation_metadata.storage_metadata_path;
    }
    else
    {
        auto result_generation_metadata = MetadataGenerator(metadata)
            .generateNextMetadata(
                filename_generator,
                metadata_name,
                parent_snapshot,
                /* added_files */total_files,
                /* added_records */total_rows,
                total_bytes,
                /* num_partitions */total_files,
                /* added_delete_files */0,
                /*num_deleted_rows*/0);
        new_snapshot = result_generation_metadata.snapshot;
        manifest_list_name = result_generation_metadata.metadata_path;
        storage_manifest_list_name = result_generation_metadata.storage_metadata_path;

    }
    auto manifest_entries_in_storage = std::make_shared<Strings>();
    Strings manifest_entries;
    Int32 manifest_lengths = 0;

    auto cleanup = [object_storage, delete_filenames, manifest_entries_in_storage, storage_manifest_list_name, storage_metadata_name]()
    {
        try
        {
            for (const auto & [_, data_file] : delete_filenames.delete_file)
                object_storage->removeObjectIfExists(StoredObject(data_file.path.path_in_storage));

            for (const auto & manifest_filename_in_storage : *manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("IcebergMutations"), "Iceberg cleanup failed");
        }
    };

    try
    {
        for (const auto & [partition_key, delete_filename] : delete_filenames.delete_file)
        {
            auto [manifest_entry_name, storage_manifest_entry_name] = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage->push_back(storage_manifest_entry_name);
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
                    chunk_partitioner ? chunk_partitioner->getColumns() : std::vector<String>{},
                    partition_key,
                    chunk_partitioner ? chunk_partitioner->getResultTypes() : std::vector<DataTypePtr>{},
                    {delete_filename.path.path_in_metadata},
                    delete_filenames.delete_statistic.at(partition_key),
                    sample_block,
                    new_snapshot,
                    configuration->format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    content_type);
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
                    content_type);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup();
                throw;
            }
        }

        if (write_metadata_json_file)
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            auto hint = filename_generator.generateVersionHint();
            if (!writeMetadataFileAndVersionHint(storage_metadata_name, json_representation, hint.path_in_storage, storage_metadata_name, object_storage, context, compression_method, configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                cleanup();
                return false;
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
    auto common_path = configuration->getRawPath().path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        FileNamesGenerator filename_generator(common_path, common_path, false, CompressionMethod::None, configuration->format);
        auto log = getLogger("IcebergMutations");
        auto [last_version, metadata_path, compression_method]
            = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);
        if (metadata->getValue<Int32>(f_format_version) < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations are supported only for the second version of iceberg format");
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
        std::optional<ChunkPartitioner> chunk_partitioner;
        if (partititon_spec->has(Iceberg::f_fields) && partititon_spec->getArray(Iceberg::f_fields)->size() > 0)
            chunk_partitioner = ChunkPartitioner(partititon_spec->getArray(Iceberg::f_fields), current_schema, context, sample_block);

        auto mutation_files = writeDataFiles(commands, context, storage_metadata, storage_id, object_storage, configuration, filename_generator, format_settings, chunk_partitioner, current_schema);

        if (mutation_files)
        {
            auto result_delete_files_metadata = writeMetadataFiles(
                mutation_files->delete_file,
                object_storage,
                configuration,
                context,
                filename_generator,
                catalog,
                storage_id,
                metadata,
                partititon_spec,
                partition_spec_id,
                chunk_partitioner,
                Iceberg::FileContentType::POSITION_DELETE,
                std::make_shared<const Block>(getPositionDeleteFileSampleBlock()),
                compression_method,
                !mutation_files->data_file);
            if (!result_delete_files_metadata)
                continue;

            if (mutation_files->data_file)
            {
                auto result_data_files_metadata = writeMetadataFiles(
                    *mutation_files->data_file,
                    object_storage,
                    configuration,
                    context,
                    filename_generator,
                    catalog,
                    storage_id,
                    metadata,
                    partititon_spec,
                    partition_spec_id,
                    chunk_partitioner,
                    Iceberg::FileContentType::DATA,
                    sample_block,
                    compression_method,
                    true);
                if (!result_data_files_metadata)
                {
                    continue;
                }
            }
        }
        break;
    }

    if (max_retries == 0)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to create iceberg snapshot");
}

void alter(
    const AlterCommands & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    size_t i = 0;
    while (i++ < MAX_TRANSACTION_RETRIES)
    {
        FileNamesGenerator filename_generator(configuration->getRawPath().path, configuration->getRawPath().path, false, CompressionMethod::None, configuration->format);
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
                if (params[0].clear)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Clear column is not supported for iceberg. Please use UPDATE instead");
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

        auto hint = filename_generator.generateVersionHint();
        if (writeMetadataFileAndVersionHint(storage_metadata_name, json_representation, hint.path_in_storage, storage_metadata_name, object_storage, context, compression_method, configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint]))
            break;
    }

    if (i == MAX_TRANSACTION_RETRIES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to alter iceberg table");
}

#endif

}
