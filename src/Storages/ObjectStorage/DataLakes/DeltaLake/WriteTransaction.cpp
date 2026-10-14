#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <base/scope_guard.h>
#include <delta_kernel_ffi.hpp>
#include <charconv>
#include <ranges>
#include <fmt/ranges.h>

#include <arrow/c/abi.h>
#include <arrow/table.h>
#include <arrow/c/bridge.h>
#include <arrow/type_fwd.h>
#include <arrow/api.h>
#include <arrow/type.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
    extern const int NETWORK_ERROR;
    extern const int DELTA_KERNEL_ERROR;
}

namespace DB::FailPoints
{
    extern const char delta_lake_commit_response_lost[];
    extern const char delta_lake_commit_conflict_with_own_entry[];
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

void exportTable(
    std::shared_ptr<arrow::Table> table,
    ffi::FFI_ArrowArray & array,
    ffi::FFI_ArrowSchema & schema)
{
    auto batch = table->CombineChunksToBatch();
    if (!batch.ok())
        DB::throwFromArrowStatus(batch.status(), DB::ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create chunks batch");

    arrow::Status status = arrow::ExportRecordBatch(
        **batch,
        reinterpret_cast<ArrowArray *>(&array),
        reinterpret_cast<ArrowSchema *>(&schema));

    if (!status.ok())
        DB::throwFromArrowStatus(status, DB::ErrorCodes::UNKNOWN_EXCEPTION, "Failed to export record batch");
}

std::shared_ptr<arrow::Table> getWriteMetadata(
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
        {std::make_shared<DB::DataTypeTuple>(
                DB::DataTypes{std::make_shared<DB::DataTypeString>()}, DB::Names{"stats_json"}), "stats"}
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
        std::string stats_json = fmt::format("{{\"numRecords\":{}}}", size_rows);
        DB::Tuple stats{stats_json};
        columns[4]->insert(stats);
    }

    DB::FormatSettings format_settings;
    auto arrow_column = std::make_unique<DB::CHColumnToArrowColumn>(
        DB::Block(names_and_types),
        "Arrow",
        DB::CHColumnToArrowColumn::Settings
        {
            /* output_string_as_string */true,
            format_settings.arrow.output_fixed_string_as_fixed_byte_array,
            format_settings.arrow.low_cardinality_as_dictionary,
            format_settings.arrow.use_signed_indexes_for_dictionary,
            format_settings.arrow.use_64_bit_indexes_for_dictionary
        });

    std::vector<DB::Chunk> meta_chunks;
    meta_chunks.emplace_back(std::move(columns), files.size());

    std::shared_ptr<arrow::Table> arrow_table;
    arrow_column->chChunkToArrowTable(arrow_table, meta_chunks, names_and_types.size());
    return arrow_table;
}

/// The kernel FFI reports a commit conflict only as this `GenericError`, as wrapped by `KernelUtils::throwError`.
/// Any other error may quote an object key, which can contain the same text.
std::optional<UInt64> tryParseConflictVersion(std::exception_ptr exception)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const DB::Exception & e)
    {
        static const std::string prefix = fmt::format(
            "Received DeltaLake kernel error {}: Generic delta kernel error: commit conflict at version ", ffi::KernelError::GenericError);
        static constexpr std::string_view suffix = " (in commit)";

        const std::string_view message = e.message();
        if (e.code() != DB::ErrorCodes::DELTA_KERNEL_ERROR || message.size() <= prefix.size() + suffix.size()
            || !message.starts_with(prefix) || !message.ends_with(suffix))
            return std::nullopt;

        const auto digits = message.substr(prefix.size(), message.size() - prefix.size() - suffix.size());
        UInt64 version = 0;
        const auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), version);
        if (ec != std::errc{} || end != digits.data() + digits.size())
            return std::nullopt;
        return version;
    }
    catch (...)
    {
        /// Ok: not a `DB::Exception`, so not a kernel error.
        return std::nullopt;
    }
}

/// Raises `message` the way the kernel FFI reports an error of `etype` from `ffi::commit`.
[[maybe_unused]] [[noreturn]] void throwAsKernelCommitError(ffi::KernelError etype, const std::string & message)
{
    DeltaLake::KernelUtils::throwError(
        DeltaLake::KernelUtils::allocateError(etype, DeltaLake::KernelUtils::toDeltaString(message)), "commit");
}

/// How many of `files` the log entry of `version` adds.
size_t countFilesAddedByLogEntry(
    const DB::IObjectStorage & object_storage,
    const std::string & table_prefix,
    UInt64 version,
    const std::vector<WriteTransaction::CommitFile> & files)
{
    auto buf = object_storage.readObject(
        DB::StoredObject(fmt::format("{}_delta_log/{:020}.json", table_prefix, version)),
        DB::getReadSettingsForMetadata());

    std::unordered_set<std::string> added_paths;
    std::string line;
    while (!buf->eof())
    {
        line.clear();
        DB::readStringUntilNewlineInto(line, *buf);
        DB::skipToNextLineOrEOF(*buf);
        if (line.empty())
            continue;

        Poco::JSON::Parser parser;
        const auto action = parser.parse(line).extract<Poco::JSON::Object::Ptr>();
        if (action && action->has("add"))
            added_paths.insert(action->getObject("add")->getValue<std::string>("path"));
    }

    return std::ranges::count_if(files, [&](const auto & file) { return added_paths.contains(file.file_name); });
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
    engine = DeltaLake::KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");

    auto * new_transaction = DeltaLake::KernelUtils::unwrapResult(
        ffi::transaction(
            DeltaLake::KernelUtils::toDeltaString(kernel_helper->getTableLocation()),
            engine.get()),
        "transaction");
    transaction = DeltaLake::KernelUtils::unwrapResult(
        ffi::with_engine_info(
            new_transaction,
            DeltaLake::KernelUtils::toDeltaString(engine_info),
            engine.get()),
        "with_engine_info");

    if (partition_columns.empty())
    {
        /// Unpartitioned tables: let the kernel build the write context.
        unpartitioned_write_context = DeltaLake::KernelUtils::unwrapResult(
            ffi::get_unpartitioned_write_context(transaction.get(), engine.get()),
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
    /// `ffi::get_write_path` would add nothing here: it returns the table root the transaction
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

void WriteTransaction::commit(const std::vector<CommitFile> & files, const DB::IObjectStorage & object_storage)
{
    assertTransactionCreated();

    LOG_TEST(log, "Will commit {} files", files.size());
    auto write_metadata = getWriteMetadata(files, log);

    ffi::FFI_ArrowArray array{};
    ffi::FFI_ArrowSchema schema{};
    SCOPE_EXIT({
        if (schema.release)
            schema.release(&schema);
    });
    exportTable(write_metadata, array, schema);

    KernelEngineData engine_data;
    try
    {
        /// Takes ownership of `array` (but not`schema`) if successfully called.
        engine_data = DeltaLake::KernelUtils::unwrapResult(
            ffi::get_engine_data(array, &schema, &KernelUtils::allocateError),
            "get_engine_data");
    }
    catch (...)
    {
        if (array.release)
            array.release(&array);
        throw;
    }

    ffi::add_files(transaction.get(), engine_data.release());

    fiu_do_on(DB::FailPoints::delta_lake_commit_fail_before_log_write, {
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "Failpoint for a commit failure before the log write enabled");
    });

    commit_outcome_unknown = true;
    try
    {
        using KernelCommittedTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveCommittedTransaction, ffi::free_committed_transaction>;
        KernelCommittedTransaction committed(DeltaLake::KernelUtils::unwrapResult(
            ffi::commit(transaction.release(), engine.get()),
            "commit"));

        auto * committed_handle = committed.get();
        auto version = ffi::committed_transaction_version(&committed_handle);

        fiu_do_on(DB::FailPoints::delta_lake_commit_response_lost, {
            throwAsKernelCommitError(ffi::KernelError::ObjectStoreError, fmt::format(
                "Error interacting with object store: Failpoint for a lost commit response, PUT {}_delta_log/{:020}.json",
                path_prefix, version));
        });

        fiu_do_on(DB::FailPoints::delta_lake_commit_conflict_with_own_entry, {
            throwAsKernelCommitError(ffi::KernelError::GenericError, fmt::format("Generic delta kernel error: commit conflict at version {}", version));
        });

        LOG_TEST(log, "Commit version: {}", version);
    }
    catch (...)
    {
        /// A conflict means the log entry exists. Data file names are unique, so it is this commit's
        /// own (a retried PUT whose response was lost) iff it adds our files; with no files it cannot be told.
        size_t files_in_entry = 0;
        std::optional<UInt64> conflict_version;
        if (!files.empty())
            conflict_version = tryParseConflictVersion(std::current_exception());
        if (conflict_version)
        {
            try
            {
                files_in_entry = countFilesAddedByLogEntry(object_storage, path_prefix, *conflict_version, files);
            }
            catch (...)
            {
                DB::tryLogCurrentException(log, "Cannot read the DeltaLake log entry of the conflicting version");
                conflict_version.reset();
            }
        }

        if (conflict_version && files_in_entry == 0)
        {
            commit_outcome_unknown = false;
            throw;
        }

        if (conflict_version && files_in_entry == files.size())
        {
            LOG_INFO(log, "DeltaLake commit reported a conflict with its own log entry of version {}, so it took effect", *conflict_version);
            commit_outcome_unknown = false;
            return;
        }

        /// `file_name` is the URI-encoded `add.path`, not the object key for partitioned tables.
        LOG_WARNING(
            log,
            "DeltaLake commit outcome is unknown, keeping the {} data file(s) written for it under {}: {}",
            files.size(),
            path_prefix,
            fmt::join(
                files | std::views::transform([](const auto & file) { return DB::unescapeForFileName(file.file_name); }),
                ", "));

        throw DB::Exception(
            DB::ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION,
            "Cannot tell whether the DeltaLake commit succeeded. The data files written for it are kept, "
            "because deleting them would destroy the rows if the commit did take effect: check the table "
            "before retrying the INSERT. Original error: {}",
            DB::getCurrentExceptionMessage(/* with_stacktrace */false));
    }
    commit_outcome_unknown = false;
}

void WriteTransaction::createTable()
{
    /// Reject non-round-tripping column types before the kernel FFI, so unsupported types raise a normal exception.
    DeltaLake::validateSchemaForDeltaCreate(table_schema);

    /// The kernel needs the table location's root directory to exist; create it up front. No-op for object stores (S3/Azure).
    kernel_helper->prepareForTableCreation();

    auto * engine_builder = kernel_helper->createBuilder();
    engine = DeltaLake::KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");

    DeltaLake::KernelCreateSchemaState schema_state;
    schema_state.schema_list = &table_schema;
    auto engine_schema = DeltaLake::buildKernelEngineSchema(schema_state);

    using KernelCreateTableBuilder = DeltaLake::KernelPointerWrapper<ffi::ExclusiveCreateTableBuilder, ffi::free_create_table_builder>;
    using KernelCreateTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveCreateTransaction, ffi::create_table_free_transaction>;
    using KernelCommittedTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveCommittedTransaction, ffi::free_committed_transaction>;

    auto builder_result = ffi::get_create_table_builder(
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
        ffi::create_table_builder_build(builder.release(), engine.get()),
        "create_table_builder_build"));

    /// `create_table_commit` likewise consumes the transaction handle.
    KernelCommittedTransaction committed(DeltaLake::KernelUtils::unwrapResult(
        ffi::create_table_commit(create_txn.release(), engine.get()),
        "create_table_commit"));
    auto * committed_handle = committed.get();
    auto version = ffi::committed_transaction_version(&committed_handle);

    LOG_TRACE(log, "Created table at version {}", version);
}

}

#endif
