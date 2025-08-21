#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Common/ArenaUtils.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Core/UUID.h>

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

        for (size_t partition_index = 0; partition_index  < partitions_size; ++partition_index)
        {
            auto & partition_chunk = partition_index_to_chunk[partition_index];
            partition_chunk.addColumn(std::move(partition_index_to_column_split[partition_index]));
        }
    }

    for (const auto & [partition_key, partition_index] : partition_id_to_chunk_index)
    {
        auto partition_data = getPartitionDataForPartitionKey(partition_key);
        auto & partition_chunk = partition_index_to_chunk[partition_index];
        partition_data->sink->consume(partition_chunk);
        partition_data->size += partition_chunk.bytes();
    }
}

DeltaLakePartitionedSink::PartitionDataPtr
DeltaLakePartitionedSink::getPartitionDataForPartitionKey(StringRef partition_key)
{
    auto it = partition_id_to_sink.find(partition_key);
    if (it == partition_id_to_sink.end())
    {
        auto data = std::make_shared<PartitionData>();
        auto data_prefix = std::filesystem::path(delta_transaction->getDataPath()) / partition_key.toString();
        data->path = DeltaLake::generateWritePath(std::move(data_prefix), configuration->format);

        data->sink = std::make_shared<StorageObjectStorageSink>(
            data->path,
            object_storage,
            configuration,
            format_settings,
            std::make_shared<Block>(partition_strategy->getFormatHeader()),
            getContext()
        );
        std::tie(it, std::ignore) = partition_id_to_sink.emplace(partition_key, std::move(data));
    }
    return it->second;
}

void DeltaLakePartitionedSink::onFinish()
{
    if (isCancelled() || partition_id_to_sink.empty())
        return;

    for (auto & [_, data] : partition_id_to_sink)
        data->sink->onFinish();

    LOG_TEST(log, "Written to {} sinks", partition_id_to_sink.size());

    try
    {
        std::vector<DeltaLake::WriteTransaction::CommitFile> files;
        files.reserve(partition_id_to_sink.size());
        const auto data_prefix = delta_transaction->getDataPath();
        for (auto & [_, data] : partition_id_to_sink)
        {
            auto keys_and_values = HivePartitioningUtils::parseHivePartitioningKeysAndValues(data->path);
            Map partition_values;
            partition_values.reserve(keys_and_values.size());
            for (const auto & [key, value] : keys_and_values)
                partition_values.emplace_back(DB::Tuple({key, value}));

            files.emplace_back(data->path.substr(data_prefix.size()), data->size, partition_values);
        }
        delta_transaction->commit(files);
    }
    catch (...)
    {
        for (auto & [_, data] : partition_id_to_sink)
        {
            object_storage->removeObjectIfExists(StoredObject(data->path));
        }
        throw;
    }
}

}

#endif
