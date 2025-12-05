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
    const Names & partition_columns_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const std::optional<FormatSettings> & format_settings_,
    const String & write_format_,
    const String & write_compression_method_)
    : SinkToStorage(sample_block_)
    , WithContext(context_)
    , log(getLogger("DeltaLakePartitionedSink"))
    , partition_columns(partition_columns_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , data_file_max_rows(context_->getSettingsRef()[Setting::delta_lake_insert_max_rows_in_data_file])
    , data_file_max_bytes(context_->getSettingsRef()[Setting::delta_lake_insert_max_bytes_in_data_file])
    , partition_strategy(createPartitionStrategy(partition_columns, getHeader(), context_))
    , delta_transaction(delta_transaction_)
    , write_format(write_format_)
    , write_compression_method(write_compression_method_)
{
    delta_transaction->validateSchema(getHeader());
}

void DeltaLakePartitionedSink::consume(Chunk & chunk)
{
    const ColumnPtr partition_by_result_column = partition_strategy->computePartitionKey(chunk);

    /// Build columns for the writer in the format header order, materializing defaults for missing non-partition columns.
    const Block format_header = partition_strategy->getFormatHeader();
    if (format_header.columns() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No column to write as all columns are partition columns");

    const auto & table_header = getHeader();
    std::unordered_map<std::string, size_t> name_to_pos;
    name_to_pos.reserve(table_header.columns());
    for (size_t i = 0; i < table_header.columns(); ++i)
        name_to_pos.emplace(table_header.getByPosition(i).name, i);

    const size_t chunk_rows = chunk.getNumRows();
    Columns default_owners;
    std::vector<const IColumn *> columns_to_consume;
    columns_to_consume.reserve(format_header.columns());

    for (size_t i = 0; i < format_header.columns(); ++i)
    {
        const auto & writer_col = format_header.getByPosition(i);
        auto it = name_to_pos.find(writer_col.name);
        if (it == name_to_pos.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Writer column '{}' not found in table header", writer_col.name);

        /// Build the columns the writer expects ("format header") from the current chunk.
        /// By this stage the pipeline should have already aligned data to the sink's input
        /// header (correct order, defaults for omitted columns).
        /// For each writer column:
        /// - if it exists in the chunk, take it by position;
        /// - otherwise, create a full default column of the correct type/size.
        ///
        /// Example:
        ///   Table header  : (a Int32, b String, c Int32, d String)
        ///   User INSERT   : INSERT INTO t (c, b) VALUES ...
        ///   Chunk at sink : (a, b, c, d)      // a,d default-filled upstream
        ///   Format header : (b, c, d)         // non-partition columns only
        /// We do the following mapping {a→0, b→1, c→2, d→3} and pick chunk[1], chunk[2], chunk[3]
        /// in the `if` branch. If the writer also expected 'e' (say added by ALTER) but the current
        /// chunk doesn't carry it yet, materialize a default column for 'e' in the `else` branch.
        if (const size_t pos_in_chunk = it->second; pos_in_chunk < chunk.getNumColumns())
        {
            columns_to_consume.emplace_back(chunk.getColumns()[pos_in_chunk].get());
        }
        else
        {
            MutableColumnPtr mut = writer_col.type->createColumn();
            mut->reserve(chunk_rows);
            for (size_t r = 0; r < chunk_rows; ++r)
                mut->insertDefault();
            default_owners.emplace_back(mut->getPtr());
            columns_to_consume.emplace_back(default_owners.back().get());
        }
    }

    chunk_row_index_to_partition_index.resize(chunk_rows);

    HashMapWithSavedHash<std::string_view, size_t> partition_id_to_chunk_index;

    for (size_t row = 0; row < chunk_rows; ++row)
    {
        auto partition_key = partition_by_result_column->getDataAt(row);
        auto [it, inserted] = partition_id_to_chunk_index.insert(makePairNoInit(partition_key, partition_id_to_chunk_index.size()));

        if (inserted)
            it->value.first = copyStringInArena(partition_keys_arena, partition_key);

        chunk_row_index_to_partition_index[row] = it->getMapped();
    }

    const size_t columns_size = columns_to_consume.size();
    size_t partitions_size = partition_id_to_chunk_index.size();

    Chunks partition_index_to_chunk;
    partition_index_to_chunk.reserve(partitions_size);

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        const IColumn * column_to_consume = columns_to_consume[column_index];
        MutableColumns partition_index_to_column_split = column_to_consume->scatter(partitions_size, chunk_row_index_to_partition_index);

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

        if (data_files.empty() || data_files.back().written_rows >= data_file_max_rows
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
DeltaLakePartitionedSink::getPartitionDataForPartitionKey(std::string_view partition_key)
{
    auto it = partitions_data.find(partition_key);
    if (it == partitions_data.end())
        std::tie(it, std::ignore) = partitions_data.emplace(partition_key, std::make_shared<PartitionInfo>(partition_key));
    return it->second;
}

DeltaLakePartitionedSink::StorageSinkPtr
DeltaLakePartitionedSink::createSinkForPartition(std::string_view partition_key)
{
    auto data_prefix = std::filesystem::path(delta_transaction->getDataPath()) / partition_key;
    return std::make_unique<StorageObjectStorageSink>(
        DeltaLake::generateWritePath(std::move(data_prefix), write_format),
        object_storage,
        format_settings,
        std::make_shared<Block>(partition_strategy->getFormatHeader()),
        getContext(),
        write_format,
        write_compression_method);
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
        std::string partition_key_str{partition_key};
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
