#include <Formats/ColumnNameMaps.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DeletionVectorWriter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionSpecResolver.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/VirtualColumnUtils.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <limits>
#include <map>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
}

namespace DB::DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
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
    Int64 total_rows{};
    Int64 total_bytes{};
};

using DataFileWriteResultByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DeleteFileWriteResult, ChunkPartitioner::PartitionKeyHasher>;
using DataFileStatisticsByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DataFileStatistics, ChunkPartitioner::PartitionKeyHasher>;

struct DataFileWriteResultWithStats
{
    DataFileWriteResultByPartitionKey files_by_partition;
    DataFileStatisticsByPartitionKey statistics_by_partition;
};

struct WriteDataFilesResult
{
    DataFileWriteResultWithStats delete_file;
    /// Empty unless this is an UPDATE, which also writes new data files.
    DataFileWriteResultWithStats data_file;
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

static Block getNonVirtualColumns(const Block & block, bool remove_low_cardinality = false)
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
        auto col_type = block.getDataTypes()[i];
        auto col_data = block.getColumns()[i];
        if (remove_low_cardinality)
        {
            col_type = removeLowCardinality(col_type);
            col_data = col_data->convertToFullColumnIfLowCardinality();
        }
        columns.push_back(ColumnWithTypeAndName(col_data, col_type, block.getNames()[i]));
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


using PartitionWriteBuffers = std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher>;
using PartitionOutputFormats = std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher>;

static void cancelAndRemoveFiles(
    PartitionOutputFormats & writers,
    PartitionWriteBuffers & buffers,
    const DataFileWriteResultByPartitionKey & files,
    const ObjectStoragePtr & object_storage,
    const Iceberg::IcebergPathResolver & path_resolver)
{
    for (auto & [_, writer] : writers)
        if (writer)
            writer->cancel();
    for (auto & [_, buffer] : buffers)
        if (buffer)
            buffer->cancel();
    for (const auto & [_, file] : files)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(file.path)));
        }
        catch (...)
        {
            tryLogCurrentException("IcebergMutations", "Cannot remove a file of a failed mutation");
        }
    }
}

static constexpr const char * row_id_column = "_row_id";
static constexpr const char * last_updated_sequence_number_column = "_last_updated_sequence_number";

static Poco::JSON::Object::Ptr addRowLineageFields(const Poco::JSON::Object::Ptr & schema)
{
    auto result = deepCopy(schema);
    auto fields = result->getArray(Iceberg::f_fields);
    for (const auto & [field_id, name] : {std::pair{Iceberg::row_id_field_id, row_id_column},
                                          std::pair{Iceberg::last_updated_sequence_number_field_id, last_updated_sequence_number_column}})
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set(Iceberg::f_id, field_id);
        field->set(Iceberg::f_name, name);
        field->set(Iceberg::f_required, false);
        field->set(Iceberg::f_type, "long");
        fields->add(field);
    }
    return result;
}

static void addRowLineageColumns(Block & data_block, const Block & source_block)
{
    const auto & source_row_ids = source_block.getByName(row_id_column).column;
    auto row_ids = ColumnInt64::create();
    auto & row_ids_data = row_ids->getData();
    row_ids_data.reserve(source_row_ids->size());
    for (size_t row = 0; row < source_row_ids->size(); ++row)
    {
        if (source_row_ids->isNullAt(row))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg format-version 3 mutation read a row without _row_id");
        row_ids_data.push_back(static_cast<Int64>(source_row_ids->getUInt(row)));
    }
    data_block.insert(ColumnWithTypeAndName(std::move(row_ids), std::make_shared<DataTypeInt64>(), row_id_column));

    const auto sequence_number_type = makeNullable(std::make_shared<DataTypeInt64>());
    data_block.insert(ColumnWithTypeAndName(
        sequence_number_type->createColumnConstWithDefaultValue(source_row_ids->size())->convertToFullColumnIfConst(),
        sequence_number_type,
        last_updated_sequence_number_column));
}

static SharedHeader addRowLineageColumns(const SharedHeader & sample_block)
{
    Block result = *sample_block;
    result.insert(ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), row_id_column));
    result.insert(ColumnWithTypeAndName(makeNullable(std::make_shared<DataTypeInt64>()), last_updated_sequence_number_column));
    return std::make_shared<const Block>(std::move(result));
}

static DataFileWriteResultWithStats writeUpdatedDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StoragePtr storage_ptr,
    ObjectStoragePtr object_storage,
    const String & write_format,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const std::optional<FormatSettings> & format_settings,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Poco::JSON::Object::Ptr data_schema,
    bool preserve_row_lineage)
{
    if (preserve_row_lineage)
        data_schema = addRowLineageFields(data_schema);

    DataFileWriteResultByPartitionKey update_data_result;
    DataFileStatisticsByPartitionKey update_data_statistics;
    PartitionWriteBuffers update_data_write_buffers;
    PartitionOutputFormats update_data_writers;

    bool written = false;
    SCOPE_EXIT({
        if (!written)
            cancelAndRemoveFiles(update_data_writers, update_data_write_buffers, update_data_result, object_storage, path_resolver);
    });

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, commands, context, settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto header = interpreter->getUpdatedHeader();
    auto update_sample_block = getNonVirtualColumns(header, /*remove_low_cardinality=*/ true);
    if (preserve_row_lineage)
        addRowLineageColumns(update_sample_block, header);

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        /// Strip virtual columns and unwrap LowCardinality to ensure the
        /// block types are compatible with the Avro serializer schema.
        auto data_block = getNonVirtualColumns(block, /*remove_low_cardinality=*/ true);
        if (preserve_row_lineage)
            addRowLineageColumns(data_block, block);
        Chunk chunk(data_block.getColumns(), data_block.rows());
        auto partition_result = getPartitionedChunks(chunk, chunk_partitioner);

        for (const auto & [partition_key, partition_chunk] : partition_result)
        {
            /// Drive the write and stats from this partition's rows only; the full `chunk`
            /// spans every partition and would duplicate rows into each per-partition file.
            auto partition_block = data_block.cloneWithColumns(partition_chunk.getColumns());

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

                ColumnMapperPtr data_column_mapper = createColumnMapper(data_schema);
                FormatFilterInfoPtr data_format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, data_column_mapper, nullptr, nullptr);
                auto data_output_format = FormatFactory::instance().getOutputFormat(
                    write_format, *data_write_buffer, partition_block, context, format_settings, data_format_filter_info);

                update_data_write_buffers[partition_key] = std::move(data_write_buffer);
                it = update_data_writers.emplace(partition_key, std::move(data_output_format)).first;
            }

            update_data_result[partition_key].total_rows += partition_block.rows();
            it->second->write(partition_block);
            update_data_statistics.at(partition_key).update(partition_chunk);
        }
    }

    for (const auto & [partition_key, _] : update_data_result)
    {
        update_data_writers[partition_key]->flush();
        update_data_writers[partition_key]->finalize();
        update_data_statistics.at(partition_key).addColumnSizesOnDisk(
            update_data_writers[partition_key]->getColumnSizesOnDisk(), update_sample_block);
        update_data_write_buffers[partition_key]->finalize();
        {
            auto update_bytes = update_data_write_buffers[partition_key]->count();
            if (update_bytes == 0)
                update_bytes = object_storage->getObjectMetadata(
                    path_resolver.resolve(update_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
            update_data_result[partition_key].total_bytes = update_bytes;
        }
        update_data_writers[partition_key].reset();
        update_data_write_buffers[partition_key].reset();
    }

    written = true;
    return DataFileWriteResultWithStats{std::move(update_data_result), std::move(update_data_statistics)};
}

static std::optional<WriteDataFilesResult> writeDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StoragePtr storage_ptr,
    ObjectStoragePtr object_storage,
    String write_format,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const std::optional<FormatSettings> & format_settings,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Poco::JSON::Object::Ptr data_schema)
{
    chassert(commands.size() == 1);
    DataFileWriteResultByPartitionKey delete_data_result;
    DataFileStatisticsByPartitionKey delete_data_statistics;
    PartitionWriteBuffers delete_data_write_buffers;
    PartitionOutputFormats delete_data_writers;

    /// No snapshot references these files until the caller commits them, so remove them if writing fails.
    bool written = false;
    SCOPE_EXIT({
        if (!written)
            cancelAndRemoveFiles(delete_data_writers, delete_data_write_buffers, delete_data_result, object_storage, path_resolver);
    });

    if (commands[0].type == MutationCommand::UPDATE || commands[0].type == MutationCommand::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto delete_commands = commands;
        delete_commands[0].type = MutationCommand::DELETE;

        /// Delete positions are written in the order of this read and must be increasing.
        auto read_context = Context::createCopy(context);
        read_context->setSetting("input_format_parquet_preserve_order", Field(true));

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, delete_commands, read_context, settings);
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
                    ColumnNameToFieldId field_ids;
                    field_ids[IcebergPositionDeleteTransform::positions_column_name] = IcebergPositionDeleteTransform::positions_column_field_id;
                    field_ids[IcebergPositionDeleteTransform::data_file_path_column_name] = IcebergPositionDeleteTransform::data_file_path_column_field_id;
                    column_mapper->setStorageColumnEncoding(std::move(field_ids));
                    /// `file_path` is Iceberg `string`, so mark it: without this the string-path-aware
                    /// writer would emit it as `binary` and produce spec-incompatible delete files.
                    column_mapper->setIcebergStringPaths({String(IcebergPositionDeleteTransform::data_file_path_column_name)});
                    FormatFilterInfoPtr format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
                    auto output_format = FormatFactory::instance().getOutputFormat(
                        write_format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

                    delete_data_write_buffers[partition_key] = std::move(write_buffer);
                    delete_data_writers[partition_key] = std::move(output_format);
                }

                col_data_filename.column = partition_chunk.getColumns()[col_data_filename_index];
                col_position.column = partition_chunk.getColumns()[col_position_index];

                /// The virtual column `_iceberg_metadata_file_path` may arrive as
                /// LowCardinality(String) from the pipeline, but the position delete
                /// file format expects plain String. Unwrap it.
                col_data_filename.column = col_data_filename.column->convertToFullColumnIfLowCardinality();
                col_data_filename.type = removeLowCardinality(col_data_filename.type);

                if (const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(col_position.column.get()))
                {
                    const auto & null_map = nullable->getNullMapData();
                    if (std::any_of(null_map.begin(), null_map.end(), [](UInt8 x) { return x != 0; }))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null _row_number");
                    col_position.column = nullable->getNestedColumnPtr();
                    col_position.type = removeNullable(col_position.type);
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
            delete_data_statistics.at(partition_key).addColumnSizesOnDisk(
                delete_data_writers[partition_key]->getColumnSizesOnDisk(), getPositionDeleteFileSampleBlock());
            delete_data_write_buffers[partition_key]->finalize();
            {
                auto delete_bytes = delete_data_write_buffers[partition_key]->count();
                if (delete_bytes == 0)
                    delete_bytes = object_storage->getObjectMetadata(
                        path_resolver.resolve(delete_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
                delete_data_result[partition_key].total_bytes = delete_bytes;
            }
            delete_data_writers[partition_key].reset();
            delete_data_write_buffers[partition_key].reset();
        }
    }

    DataFileWriteResultWithStats update_data;
    if (commands[0].type == MutationCommand::UPDATE)
        update_data = writeUpdatedDataFiles(
            commands, context, metadata, storage_ptr, object_storage, write_format, generator, path_resolver, format_settings,
            chunk_partitioner, data_schema, /* preserve_row_lineage */ false);

    std::optional<WriteDataFilesResult> result;
    if (commands[0].type == MutationCommand::DELETE)
        result = WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, {}};
    else if (commands[0].type == MutationCommand::UPDATE)
        result = WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, std::move(update_data)};
    written = true;
    return result;
}

static std::map<String, roaring::Roaring64Map> collectMatchedRowPositions(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StoragePtr storage_ptr)
{
    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;

    auto delete_commands = commands;
    delete_commands[0].type = MutationCommand::DELETE;

    MutationsInterpreter interpreter(storage_ptr, metadata, delete_commands, context, settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter.execute());
    PullingPipelineExecutor executor(pipeline);

    std::map<String, roaring::Roaring64Map> positions_by_data_file;
    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        const auto data_files = block.getByName(block_datafile_path).column->convertToFullColumnIfLowCardinality();
        auto positions = block.getByName(block_row_number).column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(positions.get()))
        {
            if (nullable->hasNull())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null _row_number");
            positions = nullable->getNestedColumnPtr();
        }

        for (size_t row = 0; row < block.rows(); ++row)
        {
            const auto data_file = data_files->getDataAt(row);
            if (data_file.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty _iceberg_metadata_file_path");
            positions_by_data_file[String(data_file)].add(positions->get64(row));
        }
    }
    return positions_by_data_file;
}

static std::optional<IcebergPathFromMetadata> getCurrentManifestListPath(const Poco::JSON::Object::Ptr & metadata)
{
    if (!metadata->has(f_current_snapshot_id) || metadata->isNull(f_current_snapshot_id))
        return std::nullopt;
    const Int64 current_snapshot_id = metadata->getValue<Int64>(f_current_snapshot_id);
    if (current_snapshot_id < 0 || !metadata->has(f_snapshots))
        return std::nullopt;

    auto snapshots = metadata->getArray(f_snapshots);
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->has(f_metadata_snapshot_id) && snapshot->getValue<Int64>(f_metadata_snapshot_id) == current_snapshot_id
            && snapshot->has(f_manifest_list))
            return IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(f_manifest_list));
    }
    return std::nullopt;
}

struct DeletionVectorChanges
{
    struct DataFile
    {
        ProcessedManifestFileEntryPtr entry;
        Int32 partition_spec_id = 0;
    };

    WrittenDeletionVectorsFile file;
    std::map<String, DataFile> data_files;
    std::unordered_set<String> data_files_with_replaced_vectors;
    std::vector<ManifestFileCacheKey> manifests_to_rewrite;
    std::unordered_set<String> carried_forward_manifest_paths;
    Int64 replaced_rows = 0;
};

static DeletionVectorChanges prepareDeletionVectorChanges(
    std::map<String, roaring::Roaring64Map> & positions_by_data_file,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    const std::optional<FormatSettings> & format_settings,
    Int32 current_schema_id,
    FileNamesGenerator & filename_generator,
    LoggerPtr log)
{
    const auto manifest_list_path = getCurrentManifestListPath(metadata);
    if (!manifest_list_path)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg mutation matched rows of a table without a current snapshot");

    DeletionVectorChanges changes;
    std::vector<ProcessedManifestFileEntryPtr> position_deletes;
    std::unordered_map<const ProcessedManifestFileEntry *, size_t> manifest_of_position_delete;

    const auto manifest_list = getManifestList(object_storage, persistent_table_components, context, *manifest_list_path, log);
    for (size_t manifest_index = 0; manifest_index < manifest_list.size(); ++manifest_index)
    {
        const auto & manifest = manifest_list[manifest_index];
        auto files_handle = getManifestFileEntriesHandle(object_storage, persistent_table_components, context, log, manifest, current_schema_id);

        for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
        {
            const String path = data_file->parsed_entry->file_path_key.serialize();
            if (positions_by_data_file.contains(path))
                changes.data_files.emplace(path, DeletionVectorChanges::DataFile{data_file, manifest.partition_spec_id});
        }
        for (const auto & position_delete : files_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
        {
            manifest_of_position_delete.emplace(position_delete.get(), manifest_index);
            position_deletes.push_back(position_delete);
        }
    }
    std::sort(position_deletes.begin(), position_deletes.end());

    std::unordered_set<size_t> manifests_to_rewrite;
    for (auto & [path, positions] : positions_by_data_file)
    {
        const auto data_file_it = changes.data_files.find(path);
        if (data_file_it == changes.data_files.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg mutation matched rows of data file {}, which is not in the current snapshot", path);
        const auto & data_file = data_file_it->second.entry;
        const auto & data_file_path = data_file->parsed_entry->file_path_key;

        auto object_info = std::make_shared<IcebergDataObjectInfo>(
            data_file, persistent_table_components.path_resolver.resolve(data_file_path), current_schema_id, std::vector<std::pair<String, Field>>{});
        for (const auto & position_delete : defineDeletesSpan(data_file, position_deletes, /* is_equality_delete */ false, log))
        {
            const auto & delete_entry = *position_delete->parsed_entry;
            if (!positionDeleteMayReferenceDataFile(delete_entry, data_file_path))
                continue;

            const auto resolved_delete_path = persistent_table_components.path_resolver.resolve(delete_entry.file_path_key);
            if (delete_entry.isDeletionVector())
            {
                if (delete_entry.referenced_data_file_path != data_file_path)
                    continue;
                object_info->addDeletionVector(position_delete, resolved_delete_path);
                changes.data_files_with_replaced_vectors.insert(path);
                changes.replaced_rows += delete_entry.record_count;
                manifests_to_rewrite.insert(manifest_of_position_delete.at(position_delete.get()));
            }
            else
            {
                object_info->addPositionDeleteFile(position_delete, resolved_delete_path);
            }
        }

        if (object_info->info.hasPositionDeletes())
            positions |= readDeletedPositions(object_info, object_storage, format_settings, context);
    }

    for (size_t manifest_index = 0; manifest_index < manifest_list.size(); ++manifest_index)
    {
        if (manifests_to_rewrite.contains(manifest_index))
            changes.manifests_to_rewrite.push_back(manifest_list[manifest_index]);
        else
            changes.carried_forward_manifest_paths.insert(manifest_list[manifest_index].manifest_file_path.serialize());
    }

    changes.file = writeDeletionVectorsFile(
        positions_by_data_file, filename_generator, persistent_table_components.path_resolver, object_storage, context);
    return changes;
}

static std::vector<DeleteManifestListEntry> writeDeletionVectorManifests(
    const DeletionVectorChanges & deletion_vectors,
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr new_snapshot,
    SharedHeader sample_block,
    FileNamesGenerator & filename_generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const PersistentTableComponents & persistent_table_components,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    Strings & written_manifests)
{
    std::vector<DeleteManifestListEntry> manifest_list_entries;

    std::map<std::pair<Int32, Int32>, std::vector<String>> data_files_by_spec;
    for (const auto & [path, data_file] : deletion_vectors.data_files)
        data_files_by_spec[{data_file.partition_spec_id, data_file.entry->resolved_schema_id}].push_back(path);

    PartitionSpecResolver partition_spec_resolver(metadata, persistent_table_components.schema_processor, context);
    for (const auto & [spec_and_schema, paths] : data_files_by_spec)
    {
        const auto & [spec_id, schema_id] = spec_and_schema;
        const auto & resolved_spec = partition_spec_resolver.resolve(spec_id, schema_id);

        std::vector<Row> partition_values;
        std::vector<Iceberg::IcebergPathFromMetadata> file_paths;
        std::vector<UInt64> row_counts;
        std::vector<UInt64> byte_counts;
        std::vector<DeletionVectorLocation> locations;
        DeleteManifestListEntry list_entry;
        list_entry.partition_spec_id = spec_id;
        if (!resolved_spec.partition_types.empty())
            list_entry.partition_summary.emplace(resolved_spec.partition_types);

        for (const auto & path : paths)
        {
            const auto & vector = deletion_vectors.file.vectors.at(path);
            const auto & partition_value = deletion_vectors.data_files.at(path).entry->normalized_partition_key_value;
            partition_values.push_back(partition_value);
            file_paths.push_back(deletion_vectors.file.path);
            row_counts.push_back(vector.cardinality);
            byte_counts.push_back(static_cast<UInt64>(deletion_vectors.file.size_in_bytes));
            locations.push_back(vector.location);
            ++list_entry.added_files_count;
            list_entry.added_rows_count += static_cast<Int64>(vector.cardinality);
            if (list_entry.partition_summary)
                list_entry.partition_summary->add(partition_value);
        }

        list_entry.path = filename_generator.generateManifestEntryName();
        written_manifests.push_back(path_resolver.resolve(list_entry.path));
        auto buffer = object_storage->writeObject(
            StoredObject(path_resolver.resolve(list_entry.path)), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        generateManifestFile(
            metadata,
            resolved_spec.partition_columns,
            partition_values,
            resolved_spec.partition_types,
            file_paths,
            row_counts,
            byte_counts,
            /* data_file_statistics */ std::nullopt,
            sample_block,
            new_snapshot,
            "puffin",
            resolved_spec.spec,
            spec_id,
            *buffer,
            Iceberg::FileContentType::POSITION_DELETE,
            /* user_defined_sequence_number */ std::nullopt,
            /* user_defined_snapshot_id */ std::nullopt,
            /* data_file_formats */ {},
            /* per_file_statistics */ {},
            /* data_file_sort_order_ids */ {},
            /* per_file_entry_lineage */ {},
            /* schema_to_serialize */ nullptr,
            /* per_file_fresh_statistics */ nullptr,
            locations);
        buffer->finalize();
        list_entry.length = static_cast<Int64>(buffer->count());
        if (list_entry.length == 0)
            list_entry.length = static_cast<Int64>(object_storage->getObjectMetadata(path_resolver.resolve(list_entry.path), /*with_tags=*/ false).size_bytes);
        manifest_list_entries.push_back(std::move(list_entry));
    }

    const Int64 new_snapshot_id = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
    Int64 removed_files = 0;
    Int64 removed_rows = 0;
    for (const auto & manifest : deletion_vectors.manifests_to_rewrite)
    {
        auto rewritten = rewriteDeleteManifest(
            manifest, deletion_vectors.data_files_with_replaced_vectors, new_snapshot_id, filename_generator, path_resolver,
            object_storage, context);
        written_manifests.push_back(path_resolver.resolve(rewritten.manifest_list_entry.path));
        removed_files += rewritten.removed_files;
        removed_rows += rewritten.removed_rows;
        manifest_list_entries.push_back(std::move(rewritten.manifest_list_entry));
    }

    if (removed_files != static_cast<Int64>(deletion_vectors.data_files_with_replaced_vectors.size()) || removed_rows != deletion_vectors.replaced_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Rewritten Iceberg delete manifests remove {} deletion vectors with {} rows, but {} with {} rows are replaced",
            removed_files,
            removed_rows,
            deletion_vectors.data_files_with_replaced_vectors.size(),
            deletion_vectors.replaced_rows);

    return manifest_list_entries;
}

static bool writeMetadataFiles(
    const DataFileWriteResultWithStats & delete_files,
    const DataFileWriteResultWithStats & data_files,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const DataLakeStorageSettings & data_lake_settings,
    String write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id,
    Poco::JSON::Object::Ptr metadata,
    const String & base_catalog_metadata_location,
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    SharedHeader data_sample_block,
    const DeletionVectorChanges * deletion_vectors,
    const PersistentTableComponents & persistent_table_components)
{
    auto delete_sample_block = std::make_shared<const Block>(getPositionDeleteFileSampleBlock());

    auto metadata_info = filename_generator.generateMetadataPathWithInfo();
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    auto sum_files = [](const DataFileWriteResultWithStats & set) -> std::tuple<Int64, Int64, Int64>
    {
        Int64 rows = 0;
        Int64 bytes = 0;
        Int64 files = 0;
        for (const auto & [_, f] : set.files_by_partition)
        {
            rows += f.total_rows;
            bytes += f.total_bytes;
            ++files;
        }
        return {rows, bytes, files};
    };

    auto [delete_rows, delete_bytes, delete_files_count] = sum_files(delete_files);
    auto [data_rows, data_bytes, data_files_count] = sum_files(data_files);

    std::unordered_set<ChunkPartitioner::PartitionKey, ChunkPartitioner::PartitionKeyHasher> touched_partitions;
    for (const auto & [partition_key, _] : delete_files.files_by_partition)
        touched_partitions.insert(partition_key);
    for (const auto & [partition_key, _] : data_files.files_by_partition)
        touched_partitions.insert(partition_key);
    Int64 num_partitions = static_cast<Int64>(touched_partitions.size());

    SnapshotRemovals removals;
    if (deletion_vectors)
    {
        std::set<std::pair<Int32, Row>> partitions_with_deletes;
        for (const auto & [_, data_file] : deletion_vectors->data_files)
            partitions_with_deletes.emplace(data_file.partition_spec_id, data_file.entry->normalized_partition_key_value);
        num_partitions = std::max(num_partitions, static_cast<Int64>(partitions_with_deletes.size()));

        delete_files_count = static_cast<Int64>(deletion_vectors->file.vectors.size());
        delete_bytes = deletion_vectors->file.size_in_bytes;
        delete_rows = 0;
        for (const auto & [_, vector] : deletion_vectors->file.vectors)
            delete_rows += static_cast<Int64>(vector.cardinality);

        removals.position_delete_files = static_cast<Int64>(deletion_vectors->data_files_with_replaced_vectors.size());
        removals.position_deletes = deletion_vectors->replaced_rows;
    }

    auto result = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator,
        metadata_info.path,
        parent_snapshot,
        /* added_files */ data_files_count,
        /* added_records */ data_rows,
        /* added_files_size */ delete_bytes + data_bytes,
        /* num_partitions */ num_partitions,
        /* added_delete_files */ delete_files_count,
        /* num_deleted_rows */ delete_rows,
        /* user_defined_snapshot_id */ std::nullopt,
        /* user_defined_timestamp */ std::nullopt,
        MetadataGenerator::SnapshotOperation::Append,
        /* refresh_cursor */ std::nullopt,
        removals);
    auto new_snapshot = result.snapshot;
    auto storage_manifest_list_name = path_resolver.resolve(result.manifest_list_path);

    auto manifest_entries_in_storage = std::make_shared<Strings>();
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_entries;
    std::vector<Int64> manifest_entry_sizes;
    std::vector<Int64> manifest_entry_row_counts;
    std::vector<Int64> manifest_entry_file_counts;
    std::vector<Iceberg::FileContentType> per_entry_content_types;
    std::vector<ManifestPartitionSummary> entry_partition_summaries;

    auto hint_path = filename_generator.generateVersionHint();
    const bool use_version_hint = data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint];

    auto cleanup = [object_storage, &delete_files, &data_files, deletion_vectors, &path_resolver, manifest_entries_in_storage,
                    storage_manifest_list_name]()
    {
        try
        {
            for (const auto & [_, data_file] : delete_files.files_by_partition)
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(data_file.path)));
            for (const auto & [_, data_file] : data_files.files_by_partition)
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(data_file.path)));
            if (deletion_vectors)
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(deletion_vectors->file.path)));

            for (const auto & manifest_filename_in_storage : *manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("IcebergMutations"), "Iceberg cleanup failed");
        }
    };

    auto write_manifest_entries = [&](const DataFileWriteResultWithStats & set, Iceberg::FileContentType content_type, SharedHeader sample_block)
    {
        for (const auto & [partition_key, data_file] : set.files_by_partition)
        {
            auto manifest_entry_path = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage->push_back(path_resolver.resolve(manifest_entry_path));
            manifest_entries.push_back(manifest_entry_path);
            per_entry_content_types.push_back(content_type);
            manifest_entry_row_counts.push_back(data_file.total_rows);
            manifest_entry_file_counts.push_back(1);

            if (chunk_partitioner)
            {
                ManifestPartitionSummary partition_summary(chunk_partitioner->getResultTypes());
                partition_summary.add(partition_key);
                entry_partition_summaries.push_back(std::move(partition_summary));
            }

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
                    {partition_key},
                    chunk_partitioner ? chunk_partitioner->getResultTypes() : DataTypes{},
                    {data_file.path},
                    {static_cast<UInt64>(data_file.total_rows)},
                    {static_cast<UInt64>(data_file.total_bytes)},
                    set.statistics_by_partition.at(partition_key),
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
    };

    try
    {
        write_manifest_entries(delete_files, Iceberg::FileContentType::POSITION_DELETE, delete_sample_block);
        write_manifest_entries(data_files, Iceberg::FileContentType::DATA, data_sample_block);

        std::vector<DeleteManifestListEntry> delete_manifest_entries;
        if (deletion_vectors)
            delete_manifest_entries = writeDeletionVectorManifests(
                *deletion_vectors, metadata, new_snapshot, data_sample_block, filename_generator, path_resolver,
                persistent_table_components, object_storage, context, *manifest_entries_in_storage);

        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            chassert(!per_entry_content_types.empty() || !delete_manifest_entries.empty());
            generateManifestList(
                path_resolver,
                metadata,
                object_storage,
                context,
                manifest_entries,
                new_snapshot,
                manifest_entry_sizes,
                *buffer_manifest_list,
                /* content_type */ Iceberg::FileContentType::POSITION_DELETE,
                /* use_previous_snapshots */ deletion_vectors == nullptr,
                per_entry_content_types,
                /* entry_counts */ {},
                /* carry_forward_manifest_paths */ deletion_vectors ? deletion_vectors->carried_forward_manifest_paths : std::unordered_set<String>{},
                /* entry_partition_spec_ids */ {},
                entry_partition_summaries,
                manifest_entry_row_counts,
                manifest_entry_file_counts,
                delete_manifest_entries);
            buffer_manifest_list->finalize();
        }

        std::string json_representation = stringifyJSON(metadata, 4);

        fiu_do_on(FailPoints::iceberg_writes_cleanup,
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
        });

        const bool catalog_writes_metadata_file = catalog && catalog->isTransactional();
        if (!catalog_writes_metadata_file)
        {
            if (!writeMetadataFileAndVersionHint(
                    path_resolver,
                    metadata_info,
                    json_representation,
                    hint_path,
                    object_storage,
                    context,
                    use_version_hint))
            {
                cleanup();
                return false;
            }
        }

        if (catalog)
        {
            auto catalog_filename = path_resolver.resolveForCatalog(metadata_info.path);
            const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
            if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, base_catalog_metadata_location, new_snapshot, metadata))
            {
                if (!catalog_writes_metadata_file)
                    removeRejectedMetadataFile(path_resolver, metadata_info, object_storage);
                cleanup();
                return false;
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

void validateMutationWriteFormat(const String & write_format)
{
    /// Fast early-reject for tables whose configured `write_format` is not Parquet.
    /// The deeper per-data-file check runs inside the `mutate` retry loop below
    /// and also handles mixed-format tables where `write_format` is Parquet but
    /// existing data files are not. See issue #102508.
    if (Poco::toLower(write_format) != "parquet")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Iceberg DELETE and UPDATE are only supported for tables with Parquet data file format, but got {}",
            write_format);
}

namespace
{
/// Validates that the current snapshot can be mutated by ClickHouse. Iceberg
/// permits mixed-format tables, but ClickHouse mutations find the deleted rows by
/// their positions in Parquet data files. A format-version 2 snapshot must not contain
/// deletion vectors, which the position delete files written for it cannot replace. Called inside
/// the `mutate` retry loop so validation is bound to the exact metadata version
/// that will be written, closing the TOCTOU gap where a concurrent writer commits
/// incompatible files after a pre-check. See issue #102508 and the PR #105893 review.
void validateSnapshotForMutation(
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    bool allow_deletion_vectors)
{
    if (!metadata->has(f_current_snapshot_id) || metadata->isNull(f_current_snapshot_id))
        return;
    Int64 current_snapshot_id = metadata->getValue<Int64>(f_current_snapshot_id);
    if (current_snapshot_id < 0)
        return;
    if (!metadata->has(f_snapshots))
        return;

    auto snapshots = metadata->getArray(f_snapshots);
    Poco::JSON::Object::Ptr current_snapshot;
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot_obj = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot_obj->has(f_metadata_snapshot_id)
            && snapshot_obj->getValue<Int64>(f_metadata_snapshot_id) == current_snapshot_id)
        {
            current_snapshot = snapshot_obj;
            break;
        }
    }
    if (!current_snapshot || !current_snapshot->has(f_manifest_list))
        return;

    auto manifest_list_path = IcebergPathFromMetadata::deserialize(current_snapshot->getValue<String>(f_manifest_list));
    auto manifest_list_entries = getManifestList(object_storage, persistent_table_components, context, manifest_list_path, log);
    for (const auto & manifest_list_entry : manifest_list_entries)
    {
        auto files_handle = getManifestFileEntriesHandle(
            object_storage, persistent_table_components, context, log, manifest_list_entry, current_schema_id);

        for (const auto & file_entry : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
        {
            const String & file_format = file_entry->parsed_entry->file_format;
            if (Poco::toLower(file_format) != "parquet")
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg DELETE and UPDATE require all data files in the current snapshot to be Parquet, "
                    "but data file `{}` has format `{}`",
                    file_entry->parsed_entry->file_path_key.serialize(),
                    file_format);
        }

        for (const auto & file_entry : files_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
        {
            if (!allow_deletion_vectors && file_entry->parsed_entry->isDeletionVector())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Iceberg DELETE and UPDATE are not supported for snapshots containing deletion vectors");
        }
    }
}
}

void mutate(
    const MutationCommands & commands,
    ContextPtr context,
    StoragePtr storage_ptr,
    StorageMetadataPtr storage_metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    validateMutationWriteFormat(write_format);

    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        auto log = getLogger("IcebergMutations");

        auto [last_version, metadata_path, compression_method, base_catalog_metadata_location] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage,
            catalog,
            storage_id.getTableName(),
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method);

        FileNamesGenerator filename_generator(
            persistent_table_components.path_resolver.getTableLocation(),
            /* use_uuid_in_metadata */ catalog != nullptr,
            CompressionMethod::None,
            write_format);
        filename_generator.setBaseMetadataFile(last_version, metadata_path);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = deepCopy(getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid));

        const auto format_version = metadata->getValue<Int32>(f_format_version);
        if (format_version < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations are supported only for Iceberg format-version 2 and later");
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

        /// Validate against the freshly-fetched snapshot. Bound to this exact
        /// metadata version, so a concurrent writer that commits non-Parquet
        /// data files between iterations is caught by the next retry.
        validateSnapshotForMutation(
            metadata, object_storage, persistent_table_components, context, log, static_cast<Int32>(current_schema_id),
            /* allow_deletion_vectors */ format_version >= 3);

        TableStateSnapshot current_iceberg_snapshot;
        current_iceberg_snapshot.metadata_file_path = metadata_path;
        current_iceberg_snapshot.metadata_version = last_version;
        current_iceberg_snapshot.schema_id = static_cast<Int32>(current_schema_id);
        if (metadata->has(Iceberg::f_current_snapshot_id))
        {
            Int64 snapshot_id_val = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
            if (snapshot_id_val >= 0)
                current_iceberg_snapshot.snapshot_id = snapshot_id_val;
        }
        auto fresh_storage_metadata = std::make_shared<StorageInMemoryMetadata>(*storage_metadata);
        fresh_storage_metadata->setDataLakeTableState(DataLakeTableStateSnapshot{current_iceberg_snapshot});

        const auto sample_block = std::make_shared<const Block>(fresh_storage_metadata->getSampleBlock());
        std::optional<ChunkPartitioner> chunk_partitioner;
        if (partititon_spec->has(Iceberg::f_fields) && partititon_spec->getArray(Iceberg::f_fields)->size() > 0)
            chunk_partitioner = ChunkPartitioner(partititon_spec->getArray(Iceberg::f_fields), current_schema->getArray(Iceberg::f_fields), context, sample_block);

        if (format_version >= 3)
        {
            auto positions_by_data_file = collectMatchedRowPositions(commands, context, fresh_storage_metadata, storage_ptr);
            if (positions_by_data_file.empty())
                break;

            DataFileWriteResultWithStats updated_data_files;
            if (commands[0].type == MutationCommand::UPDATE)
                updated_data_files = writeUpdatedDataFiles(
                    commands, context, fresh_storage_metadata, storage_ptr, object_storage, write_format, filename_generator,
                    persistent_table_components.path_resolver, format_settings, chunk_partitioner, current_schema,
                    /* preserve_row_lineage */ true);

            std::optional<DeletionVectorChanges> deletion_vectors;
            try
            {
                deletion_vectors = prepareDeletionVectorChanges(
                    positions_by_data_file, metadata, object_storage, persistent_table_components, context, format_settings,
                    static_cast<Int32>(current_schema_id), filename_generator, log);
            }
            catch (...)
            {
                PartitionOutputFormats no_writers;
                PartitionWriteBuffers no_buffers;
                cancelAndRemoveFiles(no_writers, no_buffers, updated_data_files.files_by_partition, object_storage, persistent_table_components.path_resolver);
                throw;
            }

            if (!writeMetadataFiles(
                    /* delete_files */ {},
                    updated_data_files,
                    object_storage,
                    context,
                    filename_generator,
                    persistent_table_components.path_resolver,
                    data_lake_settings,
                    write_format,
                    catalog,
                    storage_id,
                    metadata,
                    base_catalog_metadata_location,
                    partititon_spec,
                    static_cast<Int32>(partition_spec_id),
                    chunk_partitioner,
                    addRowLineageColumns(sample_block),
                    &*deletion_vectors,
                    persistent_table_components))
                continue;
            break;
        }

        auto mutation_files = writeDataFiles(
            commands,
            context,
            fresh_storage_metadata,
            storage_ptr,
            object_storage,
            write_format,
            filename_generator,
            persistent_table_components.path_resolver,
            format_settings,
            chunk_partitioner,
            current_schema);

        if (mutation_files)
        {
            if (!writeMetadataFiles(
                    mutation_files->delete_file,
                    mutation_files->data_file,
                    object_storage,
                    context,
                    filename_generator,
                    persistent_table_components.path_resolver,
                    data_lake_settings,
                    write_format,
                    catalog,
                    storage_id,
                    metadata,
                    base_catalog_metadata_location,
                    partititon_spec,
                    static_cast<Int32>(partition_spec_id),
                    chunk_partitioner,
                    sample_block,
                    /* deletion_vectors */ nullptr,
                    persistent_table_components))
                continue;
        }
        break;
    }

    if (max_retries == 0)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to create iceberg snapshot");
}

void alter(
    const AlterCommands & params,
    ContextPtr context,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    size_t i = 0;
    bool succeeded = false;
    while (i < MAX_TRANSACTION_RETRIES)
    {
        auto log = getLogger("IcebergMutations");
        auto [last_version, metadata_path, compression_method, base_catalog_metadata_location] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage,
            catalog,
            storage_id.getTableName(),
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method);

        FileNamesGenerator filename_generator(
            persistent_table_components.path_resolver.getTableLocation(),
            /* use_uuid_in_metadata */ catalog != nullptr,
            CompressionMethod::None,
            write_format);
        filename_generator.setBaseMetadataFile(last_version, metadata_path);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = deepCopy(getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid));

        const auto previous_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);

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
        setFormatVersion1SchemaAndPartitionSpec(metadata);

        const auto new_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);
        Poco::JSON::Object::Ptr new_schema;
        auto schemas = metadata->getArray(Iceberg::f_schemas);
        for (UInt32 schema_index = 0; schema_index < schemas->size(); ++schema_index)
        {
            if (schemas->getObject(schema_index)->getValue<Int32>(Iceberg::f_schema_id) == new_schema_id)
            {
                new_schema = schemas->getObject(schema_index);
                break;
            }
        }
        if (new_schema)
        {
            if (auto violation = IcebergSchemaProcessor::findSchemaViolation(*new_schema, /*is_table_schema=*/ true))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot alter the Iceberg table, its new schema would be invalid: {}", *violation);
        }

        std::string json_representation = stringifyJSON(metadata, 4);

        auto metadata_info = filename_generator.generateMetadataPathWithInfo();

        auto hint_path = filename_generator.generateVersionHint();

        const bool catalog_writes_metadata_file = catalog && catalog->isTransactional();
        if (!catalog_writes_metadata_file)
        {
            if (!writeMetadataFileAndVersionHint(
                    persistent_table_components.path_resolver,
                    metadata_info,
                    json_representation,
                    hint_path,
                    object_storage,
                    context,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                ++i;
                continue;
            }
        }

        if (catalog)
        {
            auto catalog_filename = persistent_table_components.path_resolver.resolveForCatalog(metadata_info.path);
            const auto & [namespace_name, table_name] = DataLake::parseTableName(storage_id.getTableName());
            if (!catalog->updateSchema(namespace_name, table_name, catalog_filename, base_catalog_metadata_location, new_schema, previous_schema_id))
            {
                if (!catalog_writes_metadata_file)
                    removeRejectedMetadataFile(persistent_table_components.path_resolver, metadata_info, object_storage);
                ++i;
                continue;
            }
        }

        succeeded = true;
        break;
    }

    if (!succeeded)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to alter iceberg table");

    /// Invalidate the metadata files cache so that subsequent operations on this table see the
    /// schema we just wrote. See `PersistentTableComponents::invalidateMetadataCache` for the
    /// rationale.
    persistent_table_components.invalidateMetadataCache();
}

#endif

}
