#include "DeltaLakeStorageSink.h"

#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <fmt/ranges.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

DeltaLakeStorageSink::DeltaLakeStorageSink(
    const DeltaLakeMetadataDeltaKernel & metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    SharedHeader sample_block_,
    const FormatSettings & format_settings_)
    : SinkToStorage(sample_block_)
    , log(getLogger("DeltaLakeStorageSink"))
    , format_settings(format_settings_)
    , file_name(toString(UUIDHelpers::generateV4()) + ".parquet")
{
    delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(metadata.getKernelHelper());
    /// Create a transaction.
    delta_transaction->create();
    /// Verify schema.
    const auto & write_schema = delta_transaction->getWriteSchema().getNames();
    const auto & header_schema = getHeader().getNames();
    if (write_schema != header_schema)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Header does not match write schema. Expected: {}, got: {}",
            fmt::join(write_schema, ", "), fmt::join(header_schema, ", "));
    }

    write_buf = object_storage->writeObject(
        StoredObject(std::filesystem::path(delta_transaction->getDataPath()) / file_name),
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
    if (bytes_written)
        delta_transaction->commit(file_name, bytes_written);
    else
        LOG_WARNING(log, "Nothing was written, will not commit write to delta lake");

    releaseBuffers();
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
