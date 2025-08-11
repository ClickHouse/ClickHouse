#include "DeltaLakeStorageSink.h"

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Common/logger_useful.h>

#include <Core/UUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <arrow/c/abi.h>
#include <arrow/table.h>
#include <arrow/c/bridge.h>
#include <arrow/type_fwd.h>
#include <delta_kernel_ffi.hpp>
#include <arrow/api.h>
#include <arrow/type.h>
#include <base/UUID.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

static constexpr auto engine_info = "ClickHouse";

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
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to create chunks batch: {}", batch.status().ToString());

    arrow::Status status = arrow::ExportRecordBatch(
        **batch,
        reinterpret_cast<ArrowArray *>(&array),
        reinterpret_cast<ArrowSchema *>(&schema));

    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to export record batch: {}",
            status.ToString());
    }
}

std::shared_ptr<arrow::Table> getWriteMetadata(const std::string & path, size_t size)
{
    ColumnsWithTypeAndName names_and_types{
        {std::make_shared<DataTypeString>(), "path"},
        {std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeString>()), "partitionValues"},
        {std::make_shared<DataTypeInt64>(), "size"},
        {std::make_shared<DataTypeInt64>(), "modificationTime"},
        {DataTypeFactory::instance().get("Bool"), "dataChange"},
    };

    MutableColumns columns;
    columns.reserve(names_and_types.size());
    for (const auto & [_, type, name] : names_and_types)
        columns.push_back(type->createColumn());

    columns[0]->insert(path);
    columns[1]->insertDefault();
    columns[2]->insert(size);
    columns[3]->insert(getCurrentTime());
    columns[4]->insert(true);

    FormatSettings format_settings;
    auto arrow_column = std::make_unique<CHColumnToArrowColumn>(
        Block(names_and_types),
        "Arrow",
        CHColumnToArrowColumn::Settings
        {
            /* output_string_as_string */true,
            format_settings.arrow.output_fixed_string_as_fixed_byte_array,
            format_settings.arrow.low_cardinality_as_dictionary,
            format_settings.arrow.use_signed_indexes_for_dictionary,
            format_settings.arrow.use_64_bit_indexes_for_dictionary
        });

    std::vector<Chunk> meta_chunks;
    meta_chunks.emplace_back(std::move(columns), 1);

    std::shared_ptr<arrow::Table> arrow_table;
    arrow_column->chChunkToArrowTable(arrow_table, meta_chunks, names_and_types.size());
    return arrow_table;
}

}

DeltaLakeStorageSink::DeltaLakeStorageSink(
    const DeltaLakeMetadataDeltaKernel & metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    SharedHeader sample_block_,
    const FormatSettings & format_settings_)
    : SinkToStorage(sample_block_)
    , kernel_helper(metadata.getKernelHelper())
    , log(getLogger("DeltaLakeStorageSink"))
    , format_settings(format_settings_)
    , file_name(toString(UUIDHelpers::generateV4()) + ".parquet")
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

    auto * write_path_raw = static_cast<std::string *>(
        ffi::get_write_path(write_context.get(), DeltaLake::KernelUtils::allocateString));
    if (!write_path_raw)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get write path");
    write_path = *write_path_raw;
    delete write_path_raw;

    LOG_TEST(log, "Write path: {}, file name: {}", write_path, file_name);

    write_buf = object_storage->writeObject(
        StoredObject(std::filesystem::path(kernel_helper->getDataPath()) / file_name),
        WriteMode::Rewrite,
        std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        context->getWriteSettings());

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        "Parquet",
        *write_buf,
        getHeader(),
        context,
        format_settings_);
}

void DeltaLakeStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));
}

void DeltaLakeStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();

    size_t bytes_written = write_buf->count();
    if (!bytes_written)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nothing was written");
    LOG_TEST(log, "Written bytes: {}", bytes_written);

    releaseBuffers();

    auto write_metadata = getWriteMetadata(file_name, bytes_written);

    ffi::FFI_ArrowArray array;
    ffi::FFI_ArrowSchema schema;
    exportTable(write_metadata, array, schema);

    KernelEngineData engine_data = DeltaLake::KernelUtils::unwrapResult(
        ffi::get_engine_data(array, &schema, engine.get()),
        "get_engine_data");

    ffi::add_files(transaction.get(), engine_data.release());
    auto version = DeltaLake::KernelUtils::unwrapResult(ffi::commit(transaction.release(), engine.get()), "commit");

    LOG_TEST(log, "Commit version: {}", version);
}

void DeltaLakeStorageSink::finalizeBuffers()
{
    if (!writer)
        return;

    try
    {
        writer->flush();
        writer->finalize();
    }
    catch (...)
    {
        cancelBuffers();
        releaseBuffers();
        throw;
    }

    write_buf->finalize();
}

void DeltaLakeStorageSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
}

void DeltaLakeStorageSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

}
