#include "DeltaLakeSink.h"

#include <Common/logger_useful.h>
#include <Core/UUID.h>

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

DeltaLakeSink::DeltaLakeSink(
    const DeltaLakeMetadataDeltaKernel & metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    SharedHeader sample_block_,
    const FormatSettings & format_settings_)
    : SinkToStorage(sample_block_)
    , log(getLogger("DeltaLakeSink"))
    , file_name(toString(UUIDHelpers::generateV4()) + ".parquet")
{
    delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(metadata.getKernelHelper());
    delta_transaction->create();
    delta_transaction->validateSchema(getHeader());

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

void DeltaLakeSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));
}

void DeltaLakeSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    files.emplace_back(file_name, write_buf->count(), Map{});
    delta_transaction->commit(files);

    releaseBuffers();
}

void DeltaLakeSink::finalizeBuffers()
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

void DeltaLakeSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
}

void DeltaLakeSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

}
