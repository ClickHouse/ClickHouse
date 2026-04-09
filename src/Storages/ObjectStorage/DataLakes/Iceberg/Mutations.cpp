#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/VirtualColumnUtils.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <limits>
#include <unordered_set>

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

namespace DB::Setting
{
extern const SettingsInt64 iceberg_expire_default_min_snapshots_to_keep;
extern const SettingsInt64 iceberg_expire_default_max_snapshot_age_ms;
extern const SettingsInt64 iceberg_expire_default_max_ref_age_ms;
}

namespace DB::FailPoints
{
extern const char iceberg_writes_cleanup[];
}

namespace DB::Iceberg
{

#if USE_AVRO

static constexpr const char * block_datafile_path = "_iceberg_metadata_file_path";
static constexpr const char * block_row_number = "_row_number";
static constexpr auto MAX_TRANSACTION_RETRIES = 100;

struct DeleteFileWriteResult
{
    /// Metadata path (e.g. "wasb://container@account/table/data/uuid-deletes.parquet")
    Iceberg::IcebergPathFromMetadata path;
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
    String write_format,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & path_resolver,
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
            if (block.rows() == 0)
                continue;

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
                    auto delete_file_path = generator.generatePositionDeleteFile();

                    delete_data_result[partition_key].path = delete_file_path;
                    auto write_buffer = object_storage->writeObject(
                        StoredObject(path_resolver.resolve(delete_file_path)),
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
                        write_format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

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

                /// _iceberg_metadata_file_path already contains the correct metadata path format
                /// (e.g. wasb://container@host/.../data/xxx.parquet or /iceberg/.../data/xxx.parquet)
                /// so no transformation is needed.
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
            {
                auto delete_bytes = delete_data_write_buffers[partition_key]->count();
                if (delete_bytes == 0)
                    delete_bytes = object_storage->getObjectMetadata(
                        path_resolver.resolve(delete_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
                delete_data_result[partition_key].total_bytes = static_cast<Int32>(delete_bytes);
            }
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
            if (block.rows() == 0)
                continue;

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
                    auto data_file_path = generator.generateDataFileName();
                    update_data_result[partition_key].path = data_file_path;
                    auto data_write_buffer = object_storage->writeObject(
                        StoredObject(path_resolver.resolve(data_file_path)),
                        WriteMode::Rewrite,
                        std::nullopt,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        context->getWriteSettings());

                    auto data_output_format = FormatFactory::instance().getOutputFormat(
                        write_format, *data_write_buffer, data_block, context, format_settings, nullptr);

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
            {
                auto update_bytes = update_data_write_buffers[partition_key]->count();
                if (update_bytes == 0)
                    update_bytes = object_storage->getObjectMetadata(
                        path_resolver.resolve(update_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
                update_data_result[partition_key].total_bytes = static_cast<Int32>(update_bytes);
            }
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
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const DataLakeStorageSettings & data_lake_settings,
    String write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id,
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Iceberg::FileContentType content_type,
    SharedHeader sample_block,
    bool write_metadata_json_file)
{
    auto metadata_info = filename_generator.generateMetadataPathWithInfo();
    auto storage_metadata_name = path_resolver.resolve(metadata_info.path);
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
    String storage_manifest_list_name;
    if (content_type == Iceberg::FileContentType::POSITION_DELETE)
    {
        auto result = MetadataGenerator(metadata).generateNextMetadata(
            filename_generator,
            metadata_info.path,
            parent_snapshot,
            /* added_files */ 0,
            /* added_records */ 0,
            total_bytes,
            /* num_partitions */ total_files,
            /* added_delete_files */ total_files,
            total_rows);
        new_snapshot = result.snapshot;
        storage_manifest_list_name = path_resolver.resolve(result.manifest_list_path);
    }
    else
    {
        auto result = MetadataGenerator(metadata).generateNextMetadata(
            filename_generator,
            metadata_info.path,
            parent_snapshot,
            /* added_files */ total_files,
            /* added_records */ total_rows,
            total_bytes,
            /* num_partitions */ total_files,
            /* added_delete_files */ 0,
            /*num_deleted_rows*/ 0);
        new_snapshot = result.snapshot;
        storage_manifest_list_name = path_resolver.resolve(result.manifest_list_path);
    }
    auto manifest_entries_in_storage = std::make_shared<Strings>();
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_entries;
    std::vector<Int64> manifest_entry_sizes;

    auto cleanup = [object_storage, &delete_filenames, &path_resolver, manifest_entries_in_storage, storage_manifest_list_name, storage_metadata_name]()
    {
        try
        {
            for (const auto & [_, data_file] : delete_filenames.delete_file)
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(data_file.path)));

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
            auto manifest_entry_path = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage->push_back(path_resolver.resolve(manifest_entry_path));
            manifest_entries.push_back(manifest_entry_path);

            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(path_resolver.resolve(manifest_entry_path)),
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
                    {delete_filename.path},
                    delete_filenames.delete_statistic.at(partition_key),
                    sample_block,
                    new_snapshot,
                    write_format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    content_type);
                buffer_manifest_entry->finalize();
                auto size = buffer_manifest_entry->count();
                if (size == 0)
                {
                    size = object_storage->getObjectMetadata(path_resolver.resolve(manifest_entry_path), /*with_tags=*/false).size_bytes;
                }
                manifest_entry_sizes.push_back(size);
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
                    path_resolver,
                    metadata,
                    object_storage,
                    context,
                    manifest_entries,
                    new_snapshot,
                    manifest_entry_sizes,
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

            auto hint_path = filename_generator.generateVersionHint();
            if (!writeMetadataFileAndVersionHint(
                    path_resolver,
                    metadata_info,
                    json_representation,
                    hint_path,
                    object_storage,
                    context,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                cleanup();
                return false;
            }

            if (catalog)
            {
                auto catalog_filename = path_resolver.resolveForCatalog(metadata_info.path);
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
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        auto log = getLogger("IcebergMutations");
        /// Mutations must always operate on the actual latest metadata, regardless of
        /// any explicit iceberg_metadata_file_path set on the table (used for time-travel reads).
        auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid);
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

        auto mutation_files = writeDataFiles(
            commands,
            context,
            storage_metadata,
            storage_id,
            object_storage,
            write_format,
            filename_generator,
            persistent_table_components.path_resolver,
            format_settings,
            chunk_partitioner,
            current_schema);

        if (mutation_files)
        {
            auto result_delete_files_metadata = writeMetadataFiles(
                mutation_files->delete_file,
                object_storage,
                context,
                filename_generator,
                persistent_table_components.path_resolver,
                data_lake_settings,
                write_format,
                catalog,
                storage_id,
                metadata,
                partititon_spec,
                static_cast<Int32>(partition_spec_id),
                chunk_partitioner,
                Iceberg::FileContentType::POSITION_DELETE,
                std::make_shared<const Block>(getPositionDeleteFileSampleBlock()),
                !mutation_files->data_file);
            if (!result_delete_files_metadata)
                continue;

            if (mutation_files->data_file)
            {
                auto result_data_files_metadata = writeMetadataFiles(
                    *mutation_files->data_file,
                    object_storage,
                    context,
                    filename_generator,
                    persistent_table_components.path_resolver,
                    data_lake_settings,
                    write_format,
                    catalog,
                    storage_id,
                    metadata,
                    partititon_spec,
                    static_cast<Int32>(partition_spec_id),
                    chunk_partitioner,
                    Iceberg::FileContentType::DATA,
                    sample_block,
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
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    size_t i = 0;
    while (i++ < MAX_TRANSACTION_RETRIES)
    {
        auto log = getLogger("IcebergMutations");
        auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid);

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
            case AlterCommand::Type::RENAME_COLUMN:
                metadata_json_generator.generateRenameColumnMetadata(params[0].column_name, params[0].rename_to);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type of alter {}", params[0].type);
        }

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto metadata_info = filename_generator.generateMetadataPathWithInfo();

        auto hint_path = filename_generator.generateVersionHint();
        if (writeMetadataFileAndVersionHint(
                persistent_table_components.path_resolver,
                metadata_info,
                json_representation,
                hint_path,
                object_storage,
                context,
                data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            break;
    }

    if (i == MAX_TRANSACTION_RETRIES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to alter iceberg table");
}

/// Table-level snapshot retention policy read from Iceberg table properties.
struct RetentionPolicy
{
    Int32 min_snapshots_to_keep = Iceberg::default_min_snapshots_to_keep;
    Int64 max_snapshot_age_ms = Iceberg::default_max_snapshot_age_ms;
    Int64 max_ref_age_ms = Iceberg::default_max_ref_age_ms;
};

static RetentionPolicy readRetentionPolicy(const Poco::JSON::Object::Ptr & metadata, ContextPtr context, const ExpireSnapshotsOptions & options)
{
    RetentionPolicy policy;
    const auto & settings = context->getSettingsRef();
    Int64 min_keep_from_settings = settings[Setting::iceberg_expire_default_min_snapshots_to_keep].value;
    Int64 max_snapshot_age_from_settings = settings[Setting::iceberg_expire_default_max_snapshot_age_ms].value;
    Int64 max_ref_age_from_settings = settings[Setting::iceberg_expire_default_max_ref_age_ms].value;

    if (min_keep_from_settings <= 0 || min_keep_from_settings > std::numeric_limits<Int32>::max())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_min_snapshots_to_keep must be in range [1, {}], got {}",
            std::numeric_limits<Int32>::max(),
            min_keep_from_settings);
    if (max_snapshot_age_from_settings < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_max_snapshot_age_ms must be non-negative, got {}",
            max_snapshot_age_from_settings);
    if (max_ref_age_from_settings < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_max_ref_age_ms must be non-negative, got {}",
            max_ref_age_from_settings);

    policy.min_snapshots_to_keep = static_cast<Int32>(min_keep_from_settings);
    policy.max_snapshot_age_ms = max_snapshot_age_from_settings;
    policy.max_ref_age_ms = max_ref_age_from_settings;

    if (metadata->has(Iceberg::f_properties))
    {
        auto props = metadata->getObject(Iceberg::f_properties);
        if (props->has(Iceberg::f_min_snapshots_to_keep))
            policy.min_snapshots_to_keep = std::stoi(props->getValue<String>(Iceberg::f_min_snapshots_to_keep));
        if (props->has(Iceberg::f_max_snapshot_age_ms))
            policy.max_snapshot_age_ms = std::stoll(props->getValue<String>(Iceberg::f_max_snapshot_age_ms));
        if (props->has(Iceberg::f_max_ref_age_ms))
            policy.max_ref_age_ms = std::stoll(props->getValue<String>(Iceberg::f_max_ref_age_ms));
    }

    /// Per-invocation overrides (only affect table-level defaults, not per-ref overrides).
    if (options.retain_last.has_value())
        policy.min_snapshots_to_keep = *options.retain_last;
    if (options.retention_period_ms.has_value())
        policy.max_snapshot_age_ms = *options.retention_period_ms;

    return policy;
}

/// Snapshot parent graph built from metadata, used for branch ancestor traversal.
class SnapshotGraph
{
public:
    explicit SnapshotGraph(const Poco::JSON::Array::Ptr & snapshots)
    {
        for (UInt32 i = 0; i < snapshots->size(); ++i)
        {
            auto snapshot = snapshots->getObject(i);
            Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            timestamps[snap_id] = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);
            if (snapshot->has(Iceberg::f_parent_snapshot_id) && !snapshot->isNull(Iceberg::f_parent_snapshot_id))
                parent_chain[snap_id] = snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
        }
    }

    bool hasSnapshot(Int64 snap_id) const { return timestamps.contains(snap_id); }

    Int64 getTimestamp(Int64 snap_id) const { return timestamps.at(snap_id); }

    std::optional<Int64> getParent(Int64 snap_id) const
    {
        auto it = parent_chain.find(snap_id);
        return it != parent_chain.end() ? std::optional(it->second) : std::nullopt;
    }

    /// Retain ancestors from head_id while min-keep or max-age is satisfied.
    void walkBranchAncestors(Int64 now_ms, Int64 head_id, Int32 min_keep, Int64 max_age_ms, std::set<Int64> & retained) const
    {
        Int64 walk_id = head_id;
        Int32 count = 0;
        while (hasSnapshot(walk_id))
        {
            bool within_min_keep = (count < min_keep);
            bool within_max_age = (now_ms - getTimestamp(walk_id) <= max_age_ms);
            if (!within_min_keep && !within_max_age)
                break;
            retained.insert(walk_id);
            ++count;
            auto parent = getParent(walk_id);
            if (!parent)
                break;
            walk_id = *parent;
        }
    }

private:
    std::unordered_map<Int64, Int64> parent_chain;
    std::unordered_map<Int64, Int64> timestamps;
};

/// Apply Iceberg Snapshot Retention Policy. Returns (retained IDs, expired ref names).
static std::pair<std::set<Int64>, Strings> applyRetentionPolicy(
    const Poco::JSON::Object::Ptr & metadata,
    Int64 current_snapshot_id,
    const SnapshotGraph & graph,
    const RetentionPolicy & policy,
    Int64 now_ms)
{
    std::set<Int64> retained;
    Strings expired_ref_names;
    bool main_branch_walked = false;
    if (metadata->has(Iceberg::f_refs))
    {
        auto refs = metadata->getObject(Iceberg::f_refs);
        for (const auto & ref_name : refs->getNames())
        {
            auto ref_obj = refs->getObject(ref_name);
            Int64 ref_snap_id = ref_obj->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            String ref_type = ref_obj->getValue<String>(Iceberg::f_type);

            Int64 ref_max_ref_age = ref_obj->has(Iceberg::f_ref_max_ref_age_ms)
                ? ref_obj->getValue<Int64>(Iceberg::f_ref_max_ref_age_ms)
                : policy.max_ref_age_ms;

            bool is_main = (ref_name == Iceberg::f_main);

            if (!is_main && !graph.hasSnapshot(ref_snap_id))
            {
                LOG_WARNING(getLogger("IcebergExpireSnapshots"),
                    "Removing invalid ref {}: snapshot {} does not exist", ref_name, ref_snap_id);
                expired_ref_names.push_back(ref_name);
                continue;
            }

            bool ref_expired = !is_main && (now_ms - graph.getTimestamp(ref_snap_id)) > ref_max_ref_age;

            if (ref_expired)
            {
                expired_ref_names.push_back(ref_name);
                continue;
            }

            if (ref_type == Iceberg::f_branch)
            {
                Int32 min_keep = ref_obj->has(Iceberg::f_ref_min_snapshots_to_keep)
                    ? ref_obj->getValue<Int32>(Iceberg::f_ref_min_snapshots_to_keep)
                    : policy.min_snapshots_to_keep;
                Int64 max_age = ref_obj->has(Iceberg::f_ref_max_snapshot_age_ms)
                    ? ref_obj->getValue<Int64>(Iceberg::f_ref_max_snapshot_age_ms)
                    : policy.max_snapshot_age_ms;
                graph.walkBranchAncestors(now_ms, ref_snap_id, min_keep, max_age, retained);
                if (is_main)
                    main_branch_walked = true;
            }
            else if (ref_type == Iceberg::f_tag)
            {
                retained.insert(ref_snap_id);
            }
            else
            {
                UNREACHABLE();
            }
        }
    }

    if (!main_branch_walked)
        graph.walkBranchAncestors(now_ms, current_snapshot_id, policy.min_snapshots_to_keep, policy.max_snapshot_age_ms, retained);

    return {retained, expired_ref_names};
}

static void collectAllFilePaths(
    const Iceberg::ManifestFileIterator::ManifestFileEntriesHandle & entries_handle,
    std::set<Iceberg::IcebergPathFromMetadata> & out)
{
    for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
        out.insert(entry->parsed_entry->file_path_key);
    for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
        out.insert(entry->parsed_entry->file_path_key);
    for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
        out.insert(entry->parsed_entry->file_path_key);
}

/// Collect all file paths (manifest lists, manifests, data/delete files)
/// referenced by retained snapshots.
///
/// NOTE: We only collect files with status ADDED/EXISTING (via getFilesWithoutDeleted).
/// Files with status DELETED are being removed by that snapshot and don't need retention
/// from it. A DELETED entry's data file was ADDED in an earlier snapshot — if that snapshot
/// is retained, the file is in the retained set from there; if expired, it will be collected
/// for cleanup from that snapshot's ADDED/EXISTING entries.
///
/// TODO: To handle partially-failed prior expire_snapshots (where the ADDED snapshot
/// was removed but its data files were not cleaned up), we could also traverse DELETED
/// entries in expired manifests. This requires extending ManifestFileIterator to expose
/// DELETED entries.
static void collectRetainedFiles(
    const Poco::JSON::Array::Ptr & retained_snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_paths,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_data_file_paths,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_list_paths)
{
    for (UInt32 i = 0; i < retained_snapshots->size(); ++i)
    {
        auto snapshot = retained_snapshots->getObject(i);
        if (!snapshot->has(Iceberg::f_manifest_list))
            continue;

        auto manifest_list_path = IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list));
        retained_manifest_list_paths.insert(manifest_list_path);

        auto manifest_keys = getManifestList(object_storage, persistent_table_components, context, manifest_list_path, log);

        for (const auto & mf_key : manifest_keys)
        {
            retained_manifest_paths.insert(mf_key.manifest_file_path);
            auto entries_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log,
                mf_key, current_schema_id);
            collectAllFilePaths(entries_handle, retained_data_file_paths);
        }
    }
}

struct ExpiredFiles
{
    std::vector<Iceberg::IcebergPathFromMetadata> all_paths;
    Int64 data_files = 0;
    Int64 position_delete_files = 0;
    Int64 equality_delete_files = 0;
    Int64 manifest_files = 0;
    Int64 manifest_lists = 0;
};

/// Collect files from expired snapshots that are not referenced by any retained snapshot.
static ExpiredFiles collectExpiredFiles(
    const std::vector<Iceberg::IcebergPathFromMetadata> & expired_manifest_list_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_list_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_data_file_paths,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id)
{
    ExpiredFiles result;
    std::set<Iceberg::IcebergPathFromMetadata> seen_expired_manifest_list_paths;
    std::set<Iceberg::IcebergPathFromMetadata> seen_expired_manifest_paths;
    for (const auto & manifest_list_path : expired_manifest_list_paths)
    {
        if (retained_manifest_list_paths.contains(manifest_list_path))
            continue;

        if (seen_expired_manifest_list_paths.contains(manifest_list_path))
            continue;

        ManifestFileCacheKeys manifest_keys;
        try
        {
            manifest_keys = getManifestList(object_storage, persistent_table_components, context, manifest_list_path, log);
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to read manifest list {}, skipping", manifest_list_path);
            continue;
        }

        for (const auto & mf_key : manifest_keys)
        {
            if (retained_manifest_paths.contains(mf_key.manifest_file_path))
                continue;

            if (seen_expired_manifest_paths.contains(mf_key.manifest_file_path))
                continue;

            try
            {
                auto entries_handle = getManifestFileEntriesHandle(
                    object_storage, persistent_table_components, context, log,
                    mf_key, current_schema_id);

                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
                    if (!retained_data_file_paths.contains(entry->parsed_entry->file_path_key))
                    {
                        result.all_paths.push_back(entry->parsed_entry->file_path_key);
                        ++result.data_files;
                    }
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                    if (!retained_data_file_paths.contains(entry->parsed_entry->file_path_key))
                    {
                        result.all_paths.push_back(entry->parsed_entry->file_path_key);
                        ++result.position_delete_files;
                    }
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
                    if (!retained_data_file_paths.contains(entry->parsed_entry->file_path_key))
                    {
                        result.all_paths.push_back(entry->parsed_entry->file_path_key);
                        ++result.equality_delete_files;
                    }
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed to read manifest file {}, skipping", mf_key.manifest_file_path);
                continue;
            }

            seen_expired_manifest_paths.insert(mf_key.manifest_file_path);
            result.all_paths.push_back(mf_key.manifest_file_path);
            ++result.manifest_files;
        }

        seen_expired_manifest_list_paths.insert(manifest_list_path);
        result.all_paths.push_back(manifest_list_path);
        ++result.manifest_lists;
    }
    return result;
}

/// Trim snapshot-log to the suffix of entries referencing only retained snapshots.
static void trimSnapshotLog(
    Poco::JSON::Object::Ptr metadata,
    const std::set<Int64> & expired_snapshot_ids)
{
    if (!metadata->has(Iceberg::f_snapshot_log))
        return;

    auto snapshot_log = metadata->get(Iceberg::f_snapshot_log).extract<Poco::JSON::Array::Ptr>();
    Int32 suffix_start = static_cast<Int32>(snapshot_log->size());
    for (Int32 j = static_cast<Int32>(snapshot_log->size()) - 1; j >= 0; --j)
    {
        auto entry = snapshot_log->getObject(static_cast<UInt32>(j));
        Int64 snap_id = entry->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        if (expired_snapshot_ids.contains(snap_id))
            break;
        suffix_start = j;
    }
    Poco::JSON::Array::Ptr retained_log = new Poco::JSON::Array;
    for (UInt32 j = static_cast<UInt32>(suffix_start); j < snapshot_log->size(); ++j)
        retained_log->add(snapshot_log->getObject(j));
    metadata->set(Iceberg::f_snapshot_log, retained_log);
}

struct SnapshotPartition
{
    Poco::JSON::Array::Ptr retained_snapshots = new Poco::JSON::Array;
    std::set<Int64> expired_snapshot_ids;
    std::vector<Iceberg::IcebergPathFromMetadata> expired_manifest_list_paths;
};

/// Split snapshots into retained and expired.
/// A snapshot is retained if the retention policy selected it, or if the
/// user-provided fuse timestamp protects it (snapshot newer than fuse).
static SnapshotPartition partitionSnapshots(
    const Poco::JSON::Array::Ptr & snapshots,
    const std::set<Int64> & retention_retained_ids,
    std::optional<Int64> expire_before_ms)
{
    SnapshotPartition result;
    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        Int64 snap_ts = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);

        bool is_retained_by_policy = retention_retained_ids.contains(snap_id);
        bool is_protected_by_fuse = expire_before_ms.has_value() && (snap_ts >= *expire_before_ms);

        if (is_retained_by_policy || is_protected_by_fuse)
        {
            result.retained_snapshots->add(snapshot);
        }
        else
        {
            result.expired_snapshot_ids.insert(snap_id);
            if (snapshot->has(Iceberg::f_manifest_list))
                result.expired_manifest_list_paths.push_back(
                    Iceberg::IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list)));
        }
    }
    return result;
}

static SnapshotPartition partitionSnapshotsByIds(
    const Poco::JSON::Object::Ptr & metadata,
    const Poco::JSON::Array::Ptr & snapshots,
    const std::vector<Int64> & snapshot_ids,
    Int64 current_snapshot_id,
    std::optional<Int64> expire_before_ms)
{
    std::unordered_set<Int64> requested_ids(snapshot_ids.begin(), snapshot_ids.end());
    std::unordered_set<Int64> existing_ids;
    std::unordered_set<Int64> ref_protected_ids;
    SnapshotPartition result;

    if (metadata->has(Iceberg::f_refs))
    {
        auto refs = metadata->getObject(Iceberg::f_refs);
        for (const auto & ref_name : refs->getNames())
        {
            auto ref = refs->getObject(ref_name);
            if (ref->has(Iceberg::f_metadata_snapshot_id))
                ref_protected_ids.insert(ref->getValue<Int64>(Iceberg::f_metadata_snapshot_id));
        }
    }

    ref_protected_ids.insert(current_snapshot_id);

    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        Int64 snap_ts = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);

        existing_ids.insert(snap_id);
        bool requested = requested_ids.contains(snap_id);
        bool is_protected_by_fuse = expire_before_ms.has_value() && (snap_ts >= *expire_before_ms);

        if (requested && ref_protected_ids.contains(snap_id))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "expire_snapshots cannot expire snapshot {} because it is referenced by current snapshot, branch, or tag",
                snap_id);

        if (requested && !is_protected_by_fuse)
        {
            result.expired_snapshot_ids.insert(snap_id);
            if (snapshot->has(Iceberg::f_manifest_list))
                result.expired_manifest_list_paths.push_back(Iceberg::IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list)));
        }
        else
        {
            result.retained_snapshots->add(snapshot);
        }
    }

    for (Int64 requested_id : requested_ids)
    {
        if (!existing_ids.contains(requested_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots snapshot id {} does not exist", requested_id);
    }

    return result;
}

/// Mutate metadata: remove expired refs, update snapshots, trim log, bump timestamp.
static void updateMetadataForExpiration(
    Poco::JSON::Object::Ptr metadata,
    const Strings & expired_ref_names,
    const Poco::JSON::Array::Ptr & retained_snapshots,
    const std::set<Int64> & expired_snapshot_ids)
{
    for (const auto & ref_name : expired_ref_names)
        metadata->getObject(Iceberg::f_refs)->remove(ref_name);

    metadata->set(Iceberg::f_snapshots, retained_snapshots);
    trimSnapshotLog(metadata, expired_snapshot_ids);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    metadata->set(Iceberg::f_last_updated_ms, ms.count());
}

static void deleteExpiredFiles(
    const std::vector<Iceberg::IcebergPathFromMetadata> & files_to_delete,
    const Iceberg::IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    LoggerPtr log)
{
    for (const auto & file_path : files_to_delete)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(file_path)));
            LOG_DEBUG(log, "Deleted expired file {}", file_path);
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to delete file {}: {}", file_path, getCurrentExceptionMessage(false));
        }
    }
}

/// Expire old Iceberg snapshots following the spec's Snapshot Retention Policy.
///
/// The process:
///   1. Read retention policy from table properties (with spec defaults).
///   2. Build the snapshot parent graph and determine which snapshots to retain
///      based on branch/tag refs and their min-snapshots-to-keep / max-snapshot-age-ms.
///   3. If the caller provided expire_before_ms, it acts as an additional safety
///      fuse — snapshots newer than this timestamp are never expired regardless
///      of retention policy.
///   4. Collect files exclusively owned by expired snapshots and delete them.
///   5. Write updated metadata with optimistic concurrency (retry on conflict).
ExpireSnapshotsResult expireSnapshots(
    const ExpireSnapshotsOptions & options,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name)
{
    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        auto log = getLogger("IcebergExpireSnapshots");
        auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(
            metadata_path,
            object_storage,
            persistent_table_components.metadata_cache,
            context,
            log,
            compression_method,
            persistent_table_components.table_uuid);

        if (metadata->getValue<Int32>(f_format_version) < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots is supported only for the second version of iceberg format");

        if (!metadata->has(Iceberg::f_current_snapshot_id))
        {
            LOG_INFO(log, "No snapshots to expire (table has no current snapshot)");
            return {.dry_run = options.dry_run};
        }

        Int64 current_snapshot_id = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
        if (current_snapshot_id < 0)
        {
            LOG_INFO(log, "No snapshots to expire (table has no current snapshot)");
            return {.dry_run = options.dry_run};
        }

        auto snapshots = metadata->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
        auto now_ms = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        Strings expired_ref_names;
        SnapshotPartition partition;
        if (options.snapshot_ids.has_value())
        {
            partition = partitionSnapshotsByIds(metadata, snapshots, *options.snapshot_ids, current_snapshot_id, options.expire_before_ms);
        }
        else
        {
            auto policy = readRetentionPolicy(metadata, context, options);
            SnapshotGraph graph(snapshots);
            auto [retention_retained_ids, retention_expired_ref_names] = applyRetentionPolicy(metadata, current_snapshot_id, graph, policy, now_ms);
            expired_ref_names = std::move(retention_expired_ref_names);
            partition = partitionSnapshots(snapshots, retention_retained_ids, options.expire_before_ms);
        }

        if (partition.expired_snapshot_ids.empty())
        {
            LOG_INFO(log, "No snapshots to expire");
            return {.dry_run = options.dry_run};
        }
        LOG_INFO(log, "Expiring {} snapshots", partition.expired_snapshot_ids.size());

        Int32 current_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);

        std::set<Iceberg::IcebergPathFromMetadata> retained_manifest_paths;
        std::set<Iceberg::IcebergPathFromMetadata> retained_data_file_paths;
        std::set<Iceberg::IcebergPathFromMetadata> retained_manifest_list_paths;
        collectRetainedFiles(
            partition.retained_snapshots, object_storage, persistent_table_components, context, log,
            current_schema_id, retained_manifest_paths, retained_data_file_paths, retained_manifest_list_paths);
        auto expired_files = collectExpiredFiles(
            partition.expired_manifest_list_paths, retained_manifest_list_paths, retained_manifest_paths, retained_data_file_paths,
            object_storage, persistent_table_components, context, log, current_schema_id);

        if (options.dry_run)
        {
            LOG_INFO(log, "Dry-run mode: skip metadata commit and file deletion");
            return ExpireSnapshotsResult{
                .deleted_data_files_count = expired_files.data_files,
                .deleted_position_delete_files_count = expired_files.position_delete_files,
                .deleted_equality_delete_files_count = expired_files.equality_delete_files,
                .deleted_manifest_files_count = expired_files.manifest_files,
                .deleted_manifest_lists_count = expired_files.manifest_lists,
                .dry_run = true,
            };
        }

        updateMetadataForExpiration(metadata, expired_ref_names, partition.retained_snapshots, partition.expired_snapshot_ids);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());
        auto metadata_info = filename_generator.generateMetadataPathWithInfo();
        auto hint_path = filename_generator.generateVersionHint();
        if (!writeMetadataFileAndVersionHint(
                persistent_table_components.path_resolver,
                metadata_info,
                json_representation,
                hint_path,
                object_storage,
                context,
                data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
        {
            LOG_WARNING(log, "Metadata commit conflict during expire_snapshots, retrying ({} retries left)", max_retries);
            continue;
        }

        if (catalog)
        {
            auto catalog_filename = persistent_table_components.path_resolver.resolveForCatalog(metadata_info.path);
            const auto & [namespace_name, parsed_table_name] = DataLake::parseTableName(table_name);
            if (!catalog->updateMetadata(namespace_name, parsed_table_name, catalog_filename, nullptr))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Failed to update catalog metadata after writing new metadata file. "
                    "The table metadata may be in an inconsistent state");
            }
        }

        LOG_INFO(log, "Deleting {} expired files for {} expired snapshots", expired_files.all_paths.size(), partition.expired_snapshot_ids.size());
        deleteExpiredFiles(expired_files.all_paths, persistent_table_components.path_resolver, object_storage, log);
        LOG_INFO(log, "Expired {} snapshots, deleted {} files", partition.expired_snapshot_ids.size(), expired_files.all_paths.size());

        return ExpireSnapshotsResult{
            .deleted_data_files_count = expired_files.data_files,
            .deleted_position_delete_files_count = expired_files.position_delete_files,
            .deleted_equality_delete_files_count = expired_files.equality_delete_files,
            .deleted_manifest_files_count = expired_files.manifest_files,
            .deleted_manifest_lists_count = expired_files.manifest_lists,
            .dry_run = false,
        };
    }

    if (max_retries == 0)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to expire iceberg snapshots");

    UNREACHABLE();
}

#endif

}
