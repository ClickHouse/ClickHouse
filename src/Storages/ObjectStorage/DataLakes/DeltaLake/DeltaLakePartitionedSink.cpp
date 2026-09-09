#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ArenaUtils.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <base/hex.h>
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

#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
}

namespace Setting
{
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_rows_in_data_file;
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_bytes_in_data_file;
}

namespace FailPoints
{
    extern const char delta_lake_write_commit_pause[];
}

namespace
{
    /// Delta placeholder for a missing partition value (also committed as a JSON null).
    constexpr std::string_view HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    /// delta-kernel-rs `partition::hive::needs_escaping` (non-Windows set). `-`, space and
    /// non-ASCII stay unescaped. Controls 0x00-0x1F and 0x7F are covered by the range checks.
    bool needsHiveEscaping(unsigned char c)
    {
        return c <= 0x1F || c == 0x7F
            || c == '"' || c == '#' || c == '%' || c == '\'' || c == '*' || c == '/'
            || c == ':' || c == '=' || c == '?' || c == '\\' || c == '{' || c == '[' || c == ']' || c == '^';
    }

    String hiveEscape(std::string_view s)
    {
        String result;
        result.reserve(s.size());
        for (char ch : s)
        {
            auto c = static_cast<unsigned char>(ch);
            if (needsHiveEscaping(c))
            {
                result += '%';
                result += hexDigitUppercase(c / 16);
                result += hexDigitUppercase(c % 16);
            }
            else
                result += ch;
        }
        return result;
    }

    /// Directory component `name=hiveEscape(value)`; a null-equivalent value uses the placeholder.
    String buildPartitionPathComponent(const DeltaLakePartitionedSink::PartitionValue & pv)
    {
        String component = hiveEscape(pv.name);
        component += '=';
        component += pv.is_null ? String(HIVE_DEFAULT_PARTITION) : hiveEscape(pv.value);
        return component;
    }

    /// delta-kernel-rs `partition::hive::HADOOP_URI_PATH_ENCODE_SET` (Hadoop Path.toUri()).
    bool needsUriEncoding(unsigned char c)
    {
        return c <= 0x1F || c >= 0x80
            || c == ' ' || c == '"' || c == '#' || c == '%' || c == '<' || c == '>' || c == '?'
            || c == '[' || c == '\\' || c == ']' || c == '^' || c == '`' || c == '{' || c == '|' || c == '}';
    }

    /// URI-encode the on-disk relative path for `add.path`. The reader decodes it with a single
    /// `unescapeForFileName` (which decodes every `%XX`), so one encode round-trips to the exact
    /// on-disk path. `/` separators are kept so the path structure survives.
    String uriEncodePath(const String & relative_path)
    {
        String result;
        result.reserve(relative_path.size());
        for (char ch : relative_path)
        {
            auto c = static_cast<unsigned char>(ch);
            if (c != '/' && needsUriEncoding(c))
            {
                result += '%';
                result += hexDigitUppercase(c / 16);
                result += hexDigitUppercase(c % 16);
            }
            else
                result += ch;
        }
        return result;
    }

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
            partition_columns_asts.push_back(make_intrusive<ASTIdentifier>(column));

        ASTPtr partition_by = makeASTFunction("tuple", partition_columns_asts);
        auto key_description = KeyDescription::getKeyFromAST(
            partition_by,
            ColumnsDescription(header.getNamesAndTypesList()),
            {},
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

    /// One `toString(<column>)` expression per partition column (Nullable columns yield
    /// `Nullable(String)`, keeping nulls distinguishable). Nullability is taken from the Delta
    /// write schema, which is authoritative over the user-supplied header.
    const auto & write_schema = delta_transaction->getWriteSchema();
    partition_value_actions.reserve(partition_columns.size());
    partition_column_nullable.reserve(partition_columns.size());
    for (const auto & column : partition_columns)
    {
        ASTs to_string_args{make_intrusive<ASTIdentifier>(column)};
        ASTPtr to_string_ast = makeASTFunction("toString", std::move(to_string_args));
        partition_value_actions.push_back(partition_strategy->getPartitionExpressionActions(to_string_ast));

        auto schema_column = write_schema.tryGetByName(column);
        if (!schema_column)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Partition column '{}' is not present in the DeltaLake table schema", column);
        partition_column_nullable.push_back(schema_column->type->isNullable());
    }
}

Columns DeltaLakePartitionedSink::computePartitionValueColumns(const Chunk & chunk) const
{
    Columns result;
    result.reserve(partition_value_actions.size());
    for (const auto & actions_with_column : partition_value_actions)
    {
        Block block = getHeader().cloneWithoutColumns();
        block.setColumns(chunk.getColumns());
        actions_with_column.actions->execute(block);
        result.push_back(block.getByName(actions_with_column.column_name).column->convertToFullColumnIfConst());
    }
    return result;
}

DeltaLakePartitionedSink::~DeltaLakePartitionedSink()
{
    if (isCancelled())
        cancelBuffers();
}

void DeltaLakePartitionedSink::cancelBuffers()
{
    /// The inner sinks are plain members, not pipeline processors, so the
    /// pipeline-wide cancel does not reach them. Cancel each one explicitly:
    /// this flips its isCancelled(), so its destructor finalizes/cancels its
    /// WriteBuffer instead of tripping the "neither finalized nor canceled" assert.
    /// WriteBuffer::cancel does not unlink an already written data file, so also
    /// remove each uncommitted object (mirrors the commit-failure cleanup in
    /// onFinish); otherwise a failed insert leaves orphan parquet files behind.
    for (auto & [_, partition_info] : partitions_data)
    {
        for (auto & data_file : partition_info->data_files)
        {
            data_file.sink->cancel();
            try
            {
                object_storage->removeObjectIfExists(StoredObject(data_file.sink->getPath()));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to remove uncommitted data file on cancel");
            }
        }
    }
}

void DeltaLakePartitionedSink::onException(std::exception_ptr)
{
    cancelBuffers();
}

void DeltaLakePartitionedSink::consume(Chunk & chunk)
{
    /// Serialized (toString) value of each partition column, preserving nulls.
    const Columns partition_value_columns = computePartitionValueColumns(chunk);

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

    HashMapWithSavedHash<std::string_view, size_t> partition_id_to_chunk_index;
    std::vector<PartitionValues> partition_index_to_values;

    for (size_t row = 0; row < chunk_rows; ++row)
    {
        /// Grouping key is null-tagged and embeds the escaped value, so a real NULL and the
        /// literal `__HIVE_DEFAULT_PARTITION__` (same directory) stay distinct, and values that
        /// differ only by reserved characters do not merge.
        PartitionValues row_values;
        row_values.reserve(partition_columns.size());
        String grouping_key;
        for (size_t col = 0; col < partition_columns.size(); ++col)
        {
            const IColumn & column = *partition_value_columns[col];
            /// Delta treats both SQL NULL and an empty string as null-equivalent partition values.
            const bool is_null = column.isNullAt(row);
            String value = is_null ? String{} : String{column.getDataAt(row)};
            const bool is_null_equivalent = is_null || value.empty();

            /// A null-equivalent value is committed as a JSON null in `partitionValues`; for a
            /// non-nullable partition column the reader would then fail with
            /// CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, so reject it here (delta-rs rejects it too).
            if (is_null_equivalent && !partition_column_nullable[col])
                throw Exception(
                    ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN,
                    "Cannot write {} partition value into non-nullable DeltaLake partition column '{}'",
                    is_null ? "NULL" : "empty-string", partition_columns[col]);

            PartitionValue pv;
            pv.name = partition_columns[col];
            pv.is_null = is_null_equivalent;
            if (!is_null_equivalent)
                pv.value = std::move(value);

            grouping_key += pv.is_null ? '\0' : '\1';
            grouping_key += buildPartitionPathComponent(pv);
            grouping_key += '/';
            row_values.push_back(std::move(pv));
        }

        auto [it, inserted] = partition_id_to_chunk_index.insert(
            makePairNoInit(std::string_view(grouping_key), partition_id_to_chunk_index.size()));

        if (inserted)
        {
            it->value.first = copyStringInArena(partition_keys_arena, std::string_view(grouping_key));
            partition_index_to_values.push_back(std::move(row_values));
        }

        chunk_row_index_to_partition_index[row] = it->getMapped();
    }

    size_t columns_size = columns_to_consume.size();
    size_t partitions_size = partition_id_to_chunk_index.size();

    Chunks partition_index_to_chunk;
    partition_index_to_chunk.reserve(partitions_size);

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        const IColumn * column_to_consume = columns_to_consume[column_index];
        auto partition_index_to_column_split = column_to_consume->scatter(
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
        auto & data_files = getPartitionDataForPartitionKey(partition_key, partition_index_to_values[partition_index])->data_files;
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
DeltaLakePartitionedSink::getPartitionDataForPartitionKey(std::string_view partition_key, const PartitionValues & partition_values)
{
    auto it = partitions_data.find(partition_key);
    if (it == partitions_data.end())
        std::tie(it, std::ignore) = partitions_data.emplace(partition_key, std::make_shared<PartitionInfo>(partition_key, partition_values));
    return it->second;
}

DeltaLakePartitionedSink::StorageSinkPtr
DeltaLakePartitionedSink::createSinkForPartition(std::string_view partition_key)
{
    /// The grouping key is null-tagged and not a filesystem path; build the physical Hive
    /// directory (`col=escape(value)/...`) from the true partition values instead.
    const auto & partition_values = partitions_data.at(partition_key)->partition_values;
    std::filesystem::path data_prefix(delta_transaction->getDataPath());
    for (const auto & pv : partition_values)
        data_prefix /= buildPartitionPathComponent(pv);

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
        auto & data_files = partition_info->data_files;

        /// Build `partitionValues` from the true logical values, never by parsing them back out
        /// of the path (which cannot round-trip values containing `/` or `=`). Null-equivalent
        /// values (SQL NULL and empty string) are committed as a JSON null, per the Delta protocol.
        Map partition_values;
        partition_values.reserve(partition_info->partition_values.size());
        for (const auto & pv : partition_info->partition_values)
            partition_values.emplace_back(DB::Tuple({Field(pv.name), pv.is_null ? Field() : Field(pv.value)}));

        for (const auto & [sink, written_bytes, written_rows] : data_files)
        {
            sink->onFinish();
            files.emplace_back(
                /// `add.path` is the URI-encoded on-disk path, so the reader's single
                /// `unescapeForFileName` (TableSnapshot) recovers the real relative path.
                uriEncodePath(sink->getPath().substr(data_prefix.size())),
                /// We use file size from sink to count all file, not just actual data.
                sink->getFileSize(),
                written_rows,
                partition_values);
        }
    }

    LOG_TEST(log, "Written {} data files", total_data_files_count);

    /// Test-only hook: pause inside the commit window (after the data files are
    /// finalized, before commit) so a test can inject an external cancel and
    /// check that a late cancel does not delete committed files.
    FailPointInjection::pauseFailPoint(FailPoints::delta_lake_write_commit_pause);

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

    /// The commit succeeded: the data files are now referenced by the Delta log.
    /// Drop the tracked sinks so a cancel that arrives after this point (the
    /// pipeline executor flips isCancelled() asynchronously, so it can race with
    /// this commit) does not make ~DeltaLakePartitionedSink -> cancelBuffers()
    /// unlink the just-committed files and leave the Delta log pointing at
    /// missing data.
    partitions_data.clear();
}

}

#endif
