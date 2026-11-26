#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBatchRowLimit.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>

#include <base/scope_guard.h>
#include <delta_kernel_ffi.hpp>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int NETWORK_ERROR;
}

namespace DB::FailPoints
{
    extern const char delta_lake_commit_fail_before_log_write[];
}

namespace DeltaLake
{

namespace
{

/// The Delta protocol defines `add.modificationTime` as milliseconds since the epoch.
UInt64 getCurrentTimeMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/// The add-file records of a commit, in the layout the kernel expects from `get_engine_data`.
struct WriteMetadata
{
    DB::Names names;
    DB::DataTypes types;
    DB::Columns columns;
    size_t num_rows = 0;
};

WriteMetadata getWriteMetadata(
    const std::vector<WriteTransaction::CommitFile> & files,
    LoggerPtr log)
{
    DB::ColumnsWithTypeAndName names_and_types{
        {std::make_shared<DB::DataTypeString>(), "path"},
        /// Partition values are nullable: Delta commits a JSON null for a null-equivalent
        /// partition value (SQL NULL or empty string), distinct from the string "null".
        {std::make_shared<DB::DataTypeMap>(
            std::make_shared<DB::DataTypeString>(),
            std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeString>())), "partitionValues"},
        {std::make_shared<DB::DataTypeInt64>(), "size"},
        {std::make_shared<DB::DataTypeInt64>(), "modificationTime"},
        /// The kernel JSON-encodes this struct into `add.stats`, so its field names are the
        /// committed JSON keys and must be the protocol's own.
        {std::make_shared<DB::DataTypeTuple>(
                DB::DataTypes{std::make_shared<DB::DataTypeInt64>()}, DB::Names{"numRecords"}), "stats"}
    };

    DB::MutableColumns columns;
    columns.reserve(names_and_types.size());
    for (const auto & [_, type, name] : names_and_types)
        columns.push_back(type->createColumn());

    for (const auto & [path, size_bytes, size_rows, partition_values] : files)
    {
        if (path.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Commit file path cannot be empty");

        LOG_TEST(
            log, "Committing file: {}, bytes: {}, rows: {}, partition values: {}",
            path, size_bytes, size_rows, partition_values.size());

        columns[0]->insert(path);
        columns[1]->insert(partition_values);
        columns[2]->insert(size_bytes);
        columns[3]->insert(getCurrentTimeMs());
        columns[4]->insert(DB::Tuple{DB::Field(static_cast<Int64>(size_rows))});
    }

    WriteMetadata result;
    for (size_t i = 0; i < names_and_types.size(); ++i)
    {
        result.names.push_back(names_and_types[i].name);
        result.types.push_back(names_and_types[i].type);
        result.columns.push_back(std::move(columns[i]));
    }
    result.num_rows = files.size();
    return result;
}

}

static constexpr auto engine_info = "ClickHouse";

WriteTransaction::WriteTransaction(
    DeltaLake::KernelHelperPtr kernel_helper_,
    DB::NamesAndTypesList table_schema_,
    std::unordered_set<String> table_timestamp_ntz_paths_)
    : kernel_helper(kernel_helper_)
    , table_schema(std::move(table_schema_))
    , table_timestamp_ntz_paths(std::move(table_timestamp_ntz_paths_))
    , log(getLogger("WriteTransaction"))
{
}

void WriteTransaction::assertTransactionCreated() const
{
    if (!transaction.get())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Transaction was not created");
}

const std::string & WriteTransaction::getDataPath() const
{
    assertTransactionCreated();
    return path_prefix;
}

const DB::NamesAndTypesList & WriteTransaction::getWriteSchema() const
{
    assertTransactionCreated();
    return write_schema;
}

const std::unordered_set<String> & WriteTransaction::getTimestampNtzPaths() const
{
    assertTransactionCreated();
    return write_timestamp_ntz_paths;
}

void WriteTransaction::create(const DB::Names & partition_columns)
{
    auto * engine_builder = kernel_helper->createBuilder();
    engine = DeltaLake::KernelUtils::unwrapResult(::builder_build(engine_builder), "builder_build");

    auto * new_transaction = DeltaLake::KernelUtils::unwrapResult(
        ::transaction(
            DeltaLake::KernelUtils::toDeltaString(kernel_helper->getTableLocation()),
            engine.get()),
        "transaction");
    transaction = DeltaLake::KernelUtils::unwrapResult(
        ::with_engine_info(
            new_transaction,
            DeltaLake::KernelUtils::toDeltaString(engine_info),
            engine.get()),
        "with_engine_info");

    if (partition_columns.empty())
    {
        /// Unpartitioned tables: let the kernel build the write context.
        unpartitioned_write_context = DeltaLake::KernelUtils::unwrapResult(
            ::get_unpartitioned_write_context(transaction.get(), engine.get()),
            "get_unpartitioned_write_context");
        auto write_schema_result = DeltaLake::getWriteSchema(unpartitioned_write_context.get(), engine.get());
        write_schema = std::move(write_schema_result.schema);
        write_timestamp_ntz_paths = std::move(write_schema_result.timestamp_ntz_paths);
    }
    else
    {
        /// delta-kernel exposes no partitioned write context via FFI (TODO(#2355)), so derive the
        /// write schema directly; per-partition values are handled when committing.
        write_schema = table_schema;
        write_timestamp_ntz_paths = table_timestamp_ntz_paths;
    }

    /// The reader resolves every committed `add.path` against `getDataPath`, so write under it too.
    /// `::get_write_path` would add nothing here: it returns the table root the transaction
    /// was opened with (i.e. `getTableLocation`), and exists only for the unpartitioned context.
    path_prefix = kernel_helper->getDataPath();
    /// `add.path` is the written file path with the prefix cut off, which needs the separator.
    if (!path_prefix.empty() && !path_prefix.ends_with('/'))
        path_prefix += '/';

    LOG_TEST(log, "Data prefix: {}, schema: {}", path_prefix, write_schema.toString());
}

void WriteTransaction::validateSchema(const DB::Block & header) const
{
    assertTransactionCreated();
    auto write_column_names = write_schema.getNameSet();
    auto header_column_names = header.getNamesAndTypesList().getNameSet();
    if (write_column_names != header_column_names)
    {
        /// Reachable from user input (e.g. Nested subcolumns, explicit column subsets),
        /// so this is a user error, not an internal invariant violation.
        throw DB::Exception(
            DB::ErrorCodes::INCOMPATIBLE_COLUMNS,
            "Inserted columns do not match the DeltaLake table schema. Expected: {}, got: {}",
            fmt::join(write_column_names, ", "), fmt::join(header_column_names, ", "));
    }
}

void WriteTransaction::commit(const std::vector<CommitFile> & files)
{
    assertTransactionCreated();

    LOG_TEST(log, "Will commit {} files", files.size());
    auto write_metadata = getWriteMetadata(files, log);

    /// The kernel takes record batches in the Arrow C data interface. Arrow buffers use 32-bit offsets, so metadata
    /// too large for one batch is added to the transaction as several. An empty commit still reaches the kernel as
    /// one empty batch.
    DB::FormatSettings format_settings;
    format_settings.arrow.output_string_as_string = true;
    size_t begin = 0;
    do
    {
        size_t rows = DB::maxRowsFittingOneArrowBatch(
            write_metadata.columns, write_metadata.types, begin, write_metadata.num_rows,
            format_settings.arrow.output_fixed_string_as_fixed_byte_array, format_settings.arrow.output_max_record_batch_bytes);
        /// A row that does not fit on its own cannot be represented at all; let the export reject it.
        rows = std::min(std::max<size_t>(rows, 1), write_metadata.num_rows - begin);
        const size_t end = begin + rows;

        /// The structs are declared with the C data interface types and only cast to the kernel's identical
        /// `::FFI_Arrow*` types when passed to it: the `release` callbacks must be called through the
        /// function type they are defined with, otherwise it is undefined behavior (`-fsanitize=function`).
        ::ArrowArray array{};
        ::ArrowSchema schema{};
        SCOPE_EXIT({
            if (schema.release)
                schema.release(&schema);
        });
        DB::ArrowIPC::exportSchemaToC(write_metadata.names, write_metadata.types, format_settings, schema);
        DB::ArrowIPC::exportRecordBatchToC(write_metadata.columns, write_metadata.types, format_settings, begin, end, array);
        begin = end;

        KernelEngineData engine_data;
        try
        {
            /// Takes ownership of `array` (but not `schema`) if successfully called.
            engine_data = DeltaLake::KernelUtils::unwrapResult(
                ::get_engine_data(
                    *reinterpret_cast<::FFI_ArrowArray *>(&array),
                    reinterpret_cast<::FFI_ArrowSchema *>(&schema),
                    &KernelUtils::allocateError),
                "get_engine_data");
        }
        catch (...)
        {
            if (array.release)
                array.release(&array);
            throw;
        }

        ::add_files(transaction.get(), engine_data.release());
    } while (begin < write_metadata.num_rows);

    fiu_do_on(DB::FailPoints::delta_lake_commit_fail_before_log_write, {
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "Failpoint for a commit failure before the log write enabled");
    });

    using KernelCommittedTransaction = DeltaLake::KernelPointerWrapper<::ExclusiveCommittedTransaction, ::free_committed_transaction>;
    KernelCommittedTransaction committed(DeltaLake::KernelUtils::unwrapResult(
        ::commit(transaction.release(), engine.get()),
        "commit"));
    auto * committed_handle = committed.get();
    auto version = ::committed_transaction_version(&committed_handle);

    LOG_TEST(log, "Commit version: {}", version);
}

void WriteTransaction::createTable()
{
    /// Reject non-round-tripping column types before the kernel FFI, so unsupported types raise a normal exception.
    DeltaLake::validateSchemaForDeltaCreate(table_schema);

    /// The kernel needs the table location's root directory to exist; create it up front. No-op for object stores (S3/Azure).
    kernel_helper->prepareForTableCreation();

    auto * engine_builder = kernel_helper->createBuilder();
    engine = DeltaLake::KernelUtils::unwrapResult(::builder_build(engine_builder), "builder_build");

    DeltaLake::KernelCreateSchemaState schema_state;
    schema_state.schema_list = &table_schema;
    auto engine_schema = DeltaLake::buildKernelEngineSchema(schema_state);

    using KernelCreateTableBuilder = DeltaLake::KernelPointerWrapper<::ExclusiveCreateTableBuilder, ::free_create_table_builder>;
    using KernelCreateTransaction = DeltaLake::KernelPointerWrapper<::ExclusiveCreateTransaction, ::create_table_free_transaction>;
    using KernelCommittedTransaction = DeltaLake::KernelPointerWrapper<::ExclusiveCommittedTransaction, ::free_committed_transaction>;

    auto builder_result = ::get_create_table_builder(
        DeltaLake::KernelUtils::toDeltaString(kernel_helper->getTableLocation()),
        &engine_schema,
        DeltaLake::KernelUtils::toDeltaString(engine_info),
        engine.get());
    /// Unwrap inside a `try` -- it must run either way, since it is what
    /// consumes `builder_result` -- and prefer the visitor's exception when there is one.
    KernelCreateTableBuilder builder;
    try
    {
        builder = DeltaLake::KernelUtils::unwrapResult(builder_result, "get_create_table_builder");
    }
    catch (...)
    {
        if (schema_state.exception)
            std::rethrow_exception(schema_state.exception);
        throw;
    }
    if (schema_state.exception)
        std::rethrow_exception(schema_state.exception);

    /// `create_table_builder_build` consumes the builder on both success and failure, so release() is correct here.
    KernelCreateTransaction create_txn(DeltaLake::KernelUtils::unwrapResult(
        ::create_table_builder_build(builder.release(), engine.get()),
        "create_table_builder_build"));

    /// `create_table_commit` likewise consumes the transaction handle.
    KernelCommittedTransaction committed(DeltaLake::KernelUtils::unwrapResult(
        ::create_table_commit(create_txn.release(), engine.get()),
        "create_table_commit"));
    auto * committed_handle = committed.get();
    auto version = ::committed_transaction_version(&committed_handle);

    LOG_TRACE(log, "Created table at version {}", version);
}

}

#endif
