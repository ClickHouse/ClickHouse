#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Common/ArenaUtils.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Core/UUID.h>
#include <Core/Settings.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/HivePartitioningUtils.h>

#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace Setting
{
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_rows_in_data_file;
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_bytes_in_data_file;
}

namespace
{
    /// Given partition columns list,
    /// create Hive style partition strategy with partition by expression
    /// created as Tuple function containing those columns: (a, b, ..).
    std::unique_ptr<IPartitionStrategy> createPartitionStrategy(
        const Names & partition_columns,
        const Block & header,
        ContextPtr context)
    {
        ASTs partition_columns_asts;
        for (const auto & column : partition_columns)
            partition_columns_asts.push_back(std::make_shared<ASTIdentifier>(column));

        ASTPtr partition_by = makeASTFunction("tuple", partition_columns_asts);
        auto key_description = KeyDescription::getKeyFromAST(
            partition_by,
            ColumnsDescription(header.getNamesAndTypesList()),
            context);

        return std::make_unique<HiveStylePartitionStrategy>(
            key_description,
            header,
            context,
            "parquet",
            /* partition_columns_in_data_file */false);
    }
}

DeltaLakePartitionedSink::DeltaLakePartitionedSink(
    DeltaLake::WriteTransactionPtr delta_transaction_,
    StorageObjectStorageConfigurationPtr configuration_,
    const Names & partition_columns_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const std::optional<FormatSettings> & format_settings_)
    : SinkToStorage(sample_block_)
    , WithContext(context_)
    , log(getLogger("DeltaLakePartitionedSink"))
    , partition_columns(partition_columns_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , configuration(configuration_)
    , data_file_max_rows(context_->getSettingsRef()[Setting::delta_lake_insert_max_rows_in_data_file])
    , data_file_max_bytes(context_->getSettingsRef()[Setting::delta_lake_insert_max_bytes_in_data_file])
    , partition_strategy(createPartitionStrategy(partition_columns, getHeader(), context_))
    , delta_transaction(delta_transaction_)
{
    delta_transaction->validateSchema(getHeader());
}

void DeltaLakePartitionedSink::consume(Chunk & chunk)
{
    const ColumnPtr partition_by_result_column = partition_strategy->computePartitionKey(chunk);

    /// Not all columns are serialized using the format writer
    /// (e.g, hive partitioning stores partition columns in the file path)
    const auto columns_to_consume = partition_strategy->getFormatChunkColumns(chunk);
    if (columns_to_consume.empty())
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "No column to write "
            "as all columns are specified as partition columns");
    }

    size_t chunk_rows = chunk.getNumRows();
    chunk_row_index_to_partition_index.resize(chunk_rows);

    HashMapWithSavedHash<StringRef, size_t> partition_id_to_chunk_index;

    for (size_t row = 0; row < chunk_rows; ++row)
    {
        auto partition_key = partition_by_result_column->getDataAt(row);
        auto [it, inserted] = partition_id_to_chunk_index.insert(
            makePairNoInit(partition_key, partition_id_to_chunk_index.size()));

        if (inserted)
            it->value.first = copyStringInArena(partition_keys_arena, partition_key);

        chunk_row_index_to_partition_index[row] = it->getMapped();
    }

    size_t columns_size = columns_to_consume.size();
    size_t partitions_size = partition_id_to_chunk_index.size();

    Chunks partition_index_to_chunk;
    partition_index_to_chunk.reserve(partitions_size);

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        const IColumn * column_to_consume = columns_to_consume[column_index];
        MutableColumns partition_index_to_column_split = column_to_consume->scatter(
            partitions_size,
            chunk_row_index_to_partition_index);

        /// Add chunks into partition_index_to_chunk with sizes of result columns
        if (column_index == 0)
        {
            for (const auto & partition_column : partition_index_to_column_split)
                partition_index_to_chunk.emplace_back(Columns(), partition_column->size());
        }

        for (size_t partition_index = 0; partition_index < partitions_size; ++partition_index)
        {
            auto & partition_chunk = partition_index_to_chunk[partition_index];
            partition_chunk.addColumn(std::move(partition_index_to_column_split[partition_index]));
        }
    }

    for (const auto & [partition_key, partition_index] : partition_id_to_chunk_index)
    {
        auto & data_files = getPartitionDataForPartitionKey(partition_key)->data_files;
        auto & partition_chunk = partition_index_to_chunk[partition_index];

        if (data_files.empty()
            || data_files.back().written_rows >= data_file_max_rows
            || data_files.back().written_bytes >= data_file_max_bytes)
        {
            data_files.emplace_back(createSinkForPartition(partition_key));
            total_data_files_count += 1;
        }
        auto & data_file = data_files.back();
        data_file.written_bytes += partition_chunk.bytes();
        data_file.written_rows += partition_chunk.getNumRows();
        data_file.sink->consume(partition_chunk);
    }
}

DeltaLakePartitionedSink::PartitionInfoPtr
DeltaLakePartitionedSink::getPartitionDataForPartitionKey(StringRef partition_key)
{
    auto it = partitions_data.find(partition_key);
    if (it == partitions_data.end())
        std::tie(it, std::ignore) = partitions_data.emplace(partition_key, std::make_shared<PartitionInfo>(partition_key));
    return it->second;
}

DeltaLakePartitionedSink::StorageSinkPtr
DeltaLakePartitionedSink::createSinkForPartition(StringRef partition_key)
{
    auto data_prefix = std::filesystem::path(delta_transaction->getDataPath()) / partition_key.toString();
    return std::make_unique<StorageObjectStorageSink>(
        DeltaLake::generateWritePath(std::move(data_prefix), configuration->format),
        object_storage,
        configuration,
        format_settings,
        std::make_shared<Block>(partition_strategy->getFormatHeader()),
        getContext());
}

void DeltaLakePartitionedSink::onFinish()
{
    if (isCancelled() || partitions_data.empty())
        return;

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    files.reserve(total_data_files_count);
    const auto data_prefix = delta_transaction->getDataPath();

    for (auto & [_, partition_info] : partitions_data)
    {
        auto & [partition_key, data_files] = *partition_info;
        auto partition_key_str = partition_key.toString();
        auto keys_and_values = HivePartitioningUtils::parseHivePartitioningKeysAndValues(partition_key_str);
        Map partition_values;
        partition_values.reserve(keys_and_values.size());
        for (const auto & [key, value] : keys_and_values)
            partition_values.emplace_back(DB::Tuple({key, value}));

        for (const auto & [sink, written_bytes, written_rows] : data_files)
        {
            sink->onFinish();
            files.emplace_back(
                sink->getPath().substr(data_prefix.size()),
                /// We use file size from sink to count all file, not just actual data.
                sink->getFileSize(),
                written_rows,
                partition_values);
        }
    }

    LOG_TEST(log, "Written {} data files", total_data_files_count);

    try
    {
        delta_transaction->commit(files);
    }
    catch (...)
    {
        for (auto & [_, partition_info] : partitions_data)
        {
            for (const auto & [sink, written_bytes, written_rows] : partition_info->data_files)
                object_storage->removeObjectIfExists(StoredObject(sink->getPath()));
        }
        throw;
    }
}

}

#endif
