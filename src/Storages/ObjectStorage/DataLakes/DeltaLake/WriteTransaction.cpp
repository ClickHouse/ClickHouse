#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>

#include <delta_kernel_ffi.hpp>
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
    extern const int NOT_IMPLEMENTED;
}

namespace DeltaLake
{

namespace
{

UInt64 getCurrentTime()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void exportTable(
    std::shared_ptr<arrow::Table> table,
    ffi::FFI_ArrowArray & array,
    ffi::FFI_ArrowSchema & schema)
{
    auto batch = table->CombineChunksToBatch();
    if (!batch.ok())
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to create chunks batch: {}", batch.status().ToString());

    arrow::Status status = arrow::ExportRecordBatch(
        **batch,
        reinterpret_cast<ArrowArray *>(&array),
        reinterpret_cast<ArrowSchema *>(&schema));

    if (!status.ok())
    {
        throw DB::Exception(
            DB::ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to export record batch: {}",
            status.ToString());
    }
}

std::shared_ptr<arrow::Table> getWriteMetadata(
    const std::vector<WriteTransaction::CommitFile> & files,
    LoggerPtr log)
{
    DB::ColumnsWithTypeAndName names_and_types{
        {std::make_shared<DB::DataTypeString>(), "path"},
        {std::make_shared<DB::DataTypeMap>(
            std::make_shared<DB::DataTypeString>(),
            std::make_shared<DB::DataTypeString>()), "partitionValues"},
        {std::make_shared<DB::DataTypeInt64>(), "size"},
        {std::make_shared<DB::DataTypeInt64>(), "modificationTime"},
        {DB::DataTypeFactory::instance().get("Bool"), "dataChange"},
    };

    DB::MutableColumns columns;
    columns.reserve(names_and_types.size());
    for (const auto & [_, type, name] : names_and_types)
        columns.push_back(type->createColumn());

    for (const auto & [path, size, partition_values] : files)
    {
        if (path.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Commit file path cannot be empty");

        LOG_TEST(
            log, "Committing file: {}, size: {}, partition values: {}",
            path, size, partition_values.size());

        columns[0]->insert(path);
        columns[1]->insert(partition_values);
        columns[2]->insert(size);
        columns[3]->insert(getCurrentTime());
        columns[4]->insert(true);
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

}

static constexpr auto engine_info = "ClickHouse";

WriteTransaction::WriteTransaction(DeltaLake::KernelHelperPtr kernel_helper_)
    : kernel_helper(kernel_helper_)
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

void WriteTransaction::create()
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

    write_context = ffi::get_write_context(transaction.get());
    write_schema = DeltaLake::getWriteSchema(write_context.get());

    auto * write_path_raw = static_cast<std::string *>(
        ffi::get_write_path(write_context.get(), DeltaLake::KernelUtils::allocateString));
    if (!write_path_raw)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to get write path");

    write_path = *write_path_raw;
    delete write_path_raw;

    auto pos = write_path.find("://");
    if (pos == std::string::npos)
    {
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Unexpected path format: {}", write_path);
    }
    auto storage_type_str = write_path.substr(0, pos);
    if (storage_type_str == "s3" || storage_type_str == "gcs")
    {
        auto pos_to_bucket = pos + std::strlen("://");
        auto pos_to_path = write_path.substr(pos_to_bucket).find('/');
        path_prefix = write_path.substr(pos_to_bucket + pos_to_path + 1);
    }
    else if (storage_type_str == "file")
    {
        auto pos_to_file = pos + std::strlen("://");
        path_prefix = write_path.substr(pos_to_file);
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED, "Unsupported storage type: {}",
            storage_type_str);
    }

    LOG_TEST(
        log, "Write path: {}, data prefix: {} schema: {}",
        write_path, path_prefix, write_schema.toString());
}

void WriteTransaction::validateSchema(const DB::Block & header) const
{
    assertTransactionCreated();
    auto write_column_names = write_schema.getNameSet();
    auto header_column_names = header.getNamesAndTypesList().getNameSet();
    if (write_column_names != header_column_names)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Header does not match write schema. Expected: {}, got: {}",
            fmt::join(write_column_names, ", "), fmt::join(header_column_names, ", "));
    }
}

void WriteTransaction::commit(const std::vector<CommitFile> & files)
{
    assertTransactionCreated();

    LOG_TEST(log, "Will commit {} files", files.size());
    auto write_metadata = getWriteMetadata(files, log);

    ffi::FFI_ArrowArray array;
    ffi::FFI_ArrowSchema schema;
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
            ffi::get_engine_data(array, &schema, engine.get()),
            "get_engine_data");
    }
    catch (...)
    {
        if (array.release)
            array.release(&array);
        throw;
    }

    ffi::add_files(transaction.get(), engine_data.release());
    auto version = DeltaLake::KernelUtils::unwrapResult(ffi::commit(transaction.release(), engine.get()), "commit");

    LOG_TEST(log, "Commit version: {}", version);
}

}

#endif
