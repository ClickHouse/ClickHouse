#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
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
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <deque>
#include <limits>
#include <map>
#include <unordered_set>

#include <roaring/roaring64map.hh>

namespace DB::Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}

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

/// `statistics[i]` are the statistics of `files[i]`.
struct PartitionFiles
{
    std::vector<DeleteFileWriteResult> files;
    std::vector<DataFileStatistics> statistics;
};

struct DataFileWriteResultWithStats
{
    std::unordered_map<ChunkPartitioner::PartitionKey, PartitionFiles, ChunkPartitioner::PartitionKeyHasher> files_by_partition;
};

using DeletedPositionsByDataFile = std::map<String, roaring::Roaring64Map>;

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

/// Writes the position delete file of one data file, with positions in increasing order as the specification requires.
/// Returns the pre-finalized write buffer, which the caller finalizes.
static std::unique_ptr<WriteBuffer> writePositionDeleteFile(
    const String & data_file_path,
    const roaring::Roaring64Map & positions,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const String & write_format,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const std::optional<FormatSettings> & format_settings,
    PartitionFiles & partition_files)
{
    DeleteFileWriteResult result;
    result.path = generator.generatePositionDeleteFile();

    auto write_buffer = object_storage->writeObject(
        StoredObject(path_resolver.resolve(result.path)),
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
    /// `file_path` is Iceberg `string`, so mark it: without this the string-path-aware
    /// writer would emit it as `binary` and produce spec-incompatible delete files.
    column_mapper->setIcebergStringPaths({String(IcebergPositionDeleteTransform::data_file_path_column_name)});
    FormatFilterInfoPtr format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
    auto output_format = FormatFactory::instance().getOutputFormat(
        write_format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

    DataFileStatistics statistics(IcebergPositionDeleteTransform::getSchemaFields());
    const size_t max_block_size = context->getSettingsRef()[Setting::max_block_size];
    const auto & file_path_column_description = delete_file_sample_block.getByPosition(0);
    const auto & position_column_description = delete_file_sample_block.getByPosition(1);

    const UInt64 num_positions = positions.cardinality();
    UInt64 written_positions = 0;
    ColumnPtr full_block_file_path_column;
    auto it = positions.begin();
    const auto end = positions.end();
    while (it != end)
    {
        auto position_column = ColumnInt64::create();
        auto & position_data = position_column->getData();
        position_data.reserve(std::min<UInt64>(max_block_size, num_positions - written_positions));
        for (; it != end && position_data.size() < max_block_size; ++it)
            position_data.push_back(static_cast<Int64>(*it));

        const size_t rows = position_data.size();
        written_positions += rows;
        ColumnPtr position_column_ptr = std::move(position_column);

        /// `data_file_path` comes from `_iceberg_metadata_file_path`, already in the metadata path format.
        /// The statistics of a constant column are computed without scanning it.
        ColumnPtr file_path_column = file_path_column_description.type->createColumnConst(rows, data_file_path);
        statistics.update(Chunk(Columns{file_path_column, position_column_ptr}, rows));

        if (rows == max_block_size && full_block_file_path_column)
            file_path_column = full_block_file_path_column;
        else
        {
            file_path_column = file_path_column->convertToFullColumnIfConst();
            if (rows == max_block_size)
                full_block_file_path_column = file_path_column;
        }

        output_format->write(Block({
            ColumnWithTypeAndName(file_path_column, file_path_column_description.type, file_path_column_description.name),
            ColumnWithTypeAndName(position_column_ptr, position_column_description.type, position_column_description.name)}));
        result.total_rows += rows;
    }

    output_format->flush();
    output_format->finalize();
    statistics.addColumnSizesOnDisk(output_format->getColumnSizesOnDisk(), delete_file_sample_block);
    /// Take the size before `preFinalize`, after which a small file's buffer may report 0.
    result.total_bytes = write_buffer->count();
    write_buffer->preFinalize();

    partition_files.files.push_back(std::move(result));
    partition_files.statistics.push_back(std::move(statistics));
    return write_buffer;
}

struct PendingDeleteFileUpload
{
    PartitionFiles * partition_files;
    size_t file_index;
    std::unique_ptr<WriteBuffer> write_buffer;
};

static void finishDeleteFileUpload(
    PendingDeleteFileUpload & upload, ObjectStoragePtr object_storage, const Iceberg::IcebergPathResolver & path_resolver)
{
    upload.write_buffer->finalize();
    auto & result = upload.partition_files->files[upload.file_index];
    if (result.total_bytes == 0)
        result.total_bytes = object_storage->getObjectMetadata(path_resolver.resolve(result.path), /*with_tags=*/ false).size_bytes;
    upload.write_buffer.reset();
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
    DataFileWriteResultWithStats delete_files;
    bool keep_delete_files = false;
    SCOPE_EXIT({
        if (!keep_delete_files)
        {
            for (const auto & [_, partition_files] : delete_files.files_by_partition)
                for (const auto & delete_file : partition_files.files)
                {
                    try
                    {
                        object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(delete_file.path)));
                    }
                    catch (...)
                    {
                        tryLogCurrentException(getLogger("IcebergMutations"), "Cannot remove a delete file of a failed mutation");
                    }
                }
        }
    });

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

        /// The deleted rows arrive in any order of data files and positions.
        std::unordered_map<ChunkPartitioner::PartitionKey, DeletedPositionsByDataFile, ChunkPartitioner::PartitionKeyHasher> deleted_positions;
        PaddedPODArray<UInt64> positions_buffer;

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

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                /// The virtual column `_iceberg_metadata_file_path` may arrive as LowCardinality(String).
                const IColumn & file_paths = *partition_chunk.getColumns()[col_data_filename_index];
                ColumnPtr row_numbers = partition_chunk.getColumns()[col_position_index];
                if (const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(row_numbers.get()))
                {
                    const auto & null_map = nullable->getNullMapData();
                    if (std::any_of(null_map.begin(), null_map.end(), [](UInt8 x) { return x != 0; }))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null _row_number");
                    row_numbers = nullable->getNestedColumnPtr();
                }

                /// Row numbers are non-negative.
                const size_t num_rows = partition_chunk.getNumRows();
                const UInt64 * positions = nullptr;
                if (const auto * row_numbers_int64 = typeid_cast<const ColumnInt64 *>(row_numbers.get()))
                {
                    positions = reinterpret_cast<const UInt64 *>(row_numbers_int64->getData().data());
                }
                else
                {
                    positions_buffer.resize(num_rows);
                    for (size_t row = 0; row < num_rows; ++row)
                        positions_buffer[row] = row_numbers->get64(row);
                    positions = positions_buffer.data();
                }

                /// The rows of one data file come in runs.
                const auto * low_cardinality_file_paths = typeid_cast<const ColumnLowCardinality *>(&file_paths);
                auto & positions_by_data_file = deleted_positions[partition_key];
                size_t run_begin = 0;
                while (run_begin < num_rows)
                {
                    size_t run_end = run_begin + 1;
                    if (isColumnConst(file_paths))
                    {
                        run_end = num_rows;
                    }
                    else if (low_cardinality_file_paths)
                    {
                        const IColumn & indexes = low_cardinality_file_paths->getIndexes();
                        const UInt64 index = indexes.getUInt(run_begin);
                        while (run_end < num_rows && indexes.getUInt(run_end) == index)
                            ++run_end;
                    }
                    else
                    {
                        const std::string_view path = file_paths.getDataAt(run_begin);
                        while (run_end < num_rows && file_paths.getDataAt(run_end) == path)
                            ++run_end;
                    }

                    positions_by_data_file[String(file_paths.getDataAt(run_begin))].addMany(run_end - run_begin, positions + run_begin);
                    run_begin = run_end;
                }
            }
        }

        if (!has_any_rows)
            return std::nullopt;

        static constexpr size_t max_pending_uploads = 16;
        std::deque<PendingDeleteFileUpload> pending_uploads;
        try
        {
            for (const auto & [partition_key, positions_by_data_file] : deleted_positions)
            {
                auto & partition_files = delete_files.files_by_partition[partition_key];
                for (const auto & [data_file_path, positions] : positions_by_data_file)
                {
                    if (pending_uploads.size() == max_pending_uploads)
                    {
                        finishDeleteFileUpload(pending_uploads.front(), object_storage, path_resolver);
                        pending_uploads.pop_front();
                    }
                    auto write_buffer = writePositionDeleteFile(
                        data_file_path, positions, context, object_storage, write_format, generator, path_resolver, format_settings, partition_files);
                    pending_uploads.push_back({&partition_files, partition_files.files.size() - 1, std::move(write_buffer)});
                }
            }
            for (auto & upload : pending_uploads)
                finishDeleteFileUpload(upload, object_storage, path_resolver);
        }
        catch (...)
        {
            for (auto & upload : pending_uploads)
                if (upload.write_buffer)
                    upload.write_buffer->cancel();
            throw;
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
        auto update_sample_block = getNonVirtualColumns(header, /*remove_low_cardinality=*/ true);

        Block block;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            /// Strip virtual columns and unwrap LowCardinality to ensure the
            /// block types are compatible with the Avro serializer schema.
            auto data_block = getNonVirtualColumns(block, /*remove_low_cardinality=*/ true);
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
    }

    if (commands[0].type == MutationCommand::DELETE)
    {
        keep_delete_files = true;
        return WriteDataFilesResult{std::move(delete_files), {}};
    }
    else if (commands[0].type == MutationCommand::UPDATE)
    {
        /// An UPDATE writes one data file per partition.
        DataFileWriteResultWithStats data_files;
        for (auto & [partition_key, data_file] : update_data_result)
        {
            auto & partition_files = data_files.files_by_partition[partition_key];
            partition_files.files.push_back(std::move(data_file));
            partition_files.statistics.push_back(std::move(update_data_statistics.at(partition_key)));
        }
        keep_delete_files = true;
        return WriteDataFilesResult{std::move(delete_files), std::move(data_files)};
    }
    else
        return {};
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
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    SharedHeader data_sample_block)
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
        for (const auto & [_, partition_files] : set.files_by_partition)
        {
            for (const auto & f : partition_files.files)
            {
                rows += f.total_rows;
                bytes += f.total_bytes;
                ++files;
            }
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

    auto result = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator,
        metadata_info.path,
        parent_snapshot,
        /* added_files */ data_files_count,
        /* added_records */ data_rows,
        /* added_files_size */ delete_bytes + data_bytes,
        /* num_partitions */ static_cast<Int64>(touched_partitions.size()),
        /* added_delete_files */ delete_files_count,
        /* num_deleted_rows */ delete_rows);
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

    auto cleanup = [object_storage, &delete_files, &data_files, &path_resolver, manifest_entries_in_storage,
                    storage_manifest_list_name]()
    {
        try
        {
            for (const auto * set : {&delete_files, &data_files})
                for (const auto & [_, partition_files] : set->files_by_partition)
                    for (const auto & data_file : partition_files.files)
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

    auto write_manifest_entries = [&](const DataFileWriteResultWithStats & set, Iceberg::FileContentType content_type, SharedHeader sample_block)
    {
        /// One manifest per partition, with an entry and the statistics of each of its files.
        for (const auto & [partition_key, partition_files] : set.files_by_partition)
        {
            std::vector<Iceberg::IcebergPathFromMetadata> file_paths;
            std::vector<UInt64> file_row_counts;
            std::vector<UInt64> file_byte_counts;
            std::vector<const DataFileStatistics *> file_statistics;
            Int64 partition_rows = 0;
            for (size_t i = 0; i < partition_files.files.size(); ++i)
            {
                const auto & data_file = partition_files.files[i];
                file_paths.push_back(data_file.path);
                file_row_counts.push_back(static_cast<UInt64>(data_file.total_rows));
                file_byte_counts.push_back(static_cast<UInt64>(data_file.total_bytes));
                file_statistics.push_back(&partition_files.statistics[i]);
                partition_rows += data_file.total_rows;
            }

            auto manifest_entry_path = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage->push_back(path_resolver.resolve(manifest_entry_path));
            manifest_entries.push_back(manifest_entry_path);
            per_entry_content_types.push_back(content_type);
            manifest_entry_row_counts.push_back(partition_rows);
            manifest_entry_file_counts.push_back(static_cast<Int64>(partition_files.files.size()));

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
                    std::vector<Row>(file_paths.size(), partition_key),
                    chunk_partitioner ? chunk_partitioner->getResultTypes() : DataTypes{},
                    file_paths,
                    file_row_counts,
                    file_byte_counts,
                    /* data_file_statistics */ std::nullopt,
                    sample_block,
                    new_snapshot,
                    write_format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    content_type,
                    /* user_defined_sequence_number */ std::nullopt,
                    /* user_defined_snapshot_id */ std::nullopt,
                    /* data_file_formats */ {},
                    /* per_file_statistics */ {},
                    /* data_file_sort_order_ids */ {},
                    /* per_file_entry_lineage */ {},
                    /* schema_to_serialize */ nullptr,
                    &file_statistics);
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

        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            chassert(!per_entry_content_types.empty());
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
                /* use_previous_snapshots */ true,
                per_entry_content_types,
                /* entry_counts */ {},
                /* carry_forward_manifest_paths */ {},
                /* entry_partition_spec_ids */ {},
                entry_partition_summaries,
                manifest_entry_row_counts,
                manifest_entry_file_counts);
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
            if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
            {
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
/// permits mixed-format tables, but ClickHouse mutations write Parquet position
/// delete files. A snapshot containing non-Parquet data files or deletion vectors
/// would produce delete files that the read side cannot safely apply. Called inside
/// the `mutate` retry loop so validation is bound to the exact metadata version
/// that will be written, closing the TOCTOU gap where a concurrent writer commits
/// incompatible files after a pre-check. See issue #102508 and the PR #105893 review.
void validateSnapshotForMutation(
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id)
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
            if (file_entry->parsed_entry->isDeletionVector())
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

        auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
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

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = deepCopy(getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid));

        const auto format_version = metadata->getValue<Int32>(f_format_version);
        if (format_version < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations are supported only for the second version of iceberg format");
        if (format_version >= 3)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Iceberg DELETE and UPDATE are not supported for format-version 3 tables");
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
            metadata, object_storage, persistent_table_components, context, log, static_cast<Int32>(current_schema_id));

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
                    partititon_spec,
                    static_cast<Int32>(partition_spec_id),
                    chunk_partitioner,
                    sample_block))
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
        auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
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

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
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
            if (!catalog->updateSchema(namespace_name, table_name, catalog_filename, new_schema, previous_schema_id))
            {
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
