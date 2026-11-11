#include <Storages/MergeTree/RowsSourcesTemporaryFile.h>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RowsSourcesOnTemporaryDisk::RowsSourcesOnTemporaryDisk(TemporaryDataOnDiskScopePtr temporary_data_on_disk_)
    : temporary_data_on_disk(temporary_data_on_disk_->childScope({CurrentMetrics::TemporaryFilesForMerge}))
{
}

WriteBuffer & RowsSourcesOnTemporaryDisk::getTemporaryFileForWriting(const String & name)
{
    if (name != FILE_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

    if (tmp_data_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was already requested for writing, there musto be only one writer");

    tmp_data_buffer = std::make_unique<TemporaryDataBuffer>(temporary_data_on_disk);
    return *tmp_data_buffer;
}

std::unique_ptr<ReadBuffer> RowsSourcesOnTemporaryDisk::getTemporaryFileForReading(const String & name)
{
    if (name != FILE_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

    if (!finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file is not finalized yet");

    /// tmp_disk might not create real file if no data was written to it.
    if (final_size == 0)
        return std::make_unique<ReadBufferFromEmptyFile>();

    /// Reopen the file for each read so that multiple reads can be performed in parallel and there is no need to seek to the beginning.
    return tmp_data_buffer->read();
}

size_t RowsSourcesOnTemporaryDisk::finalizeWriting()
{
    if (!tmp_data_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was not requested for writing");

    auto stat = tmp_data_buffer->finishWriting();
    finalized = true;
    final_size = stat.uncompressed_size;
    return final_size;
}

RowsSourcesInPart::RowsSourcesInPart(MutableDataPartStoragePtr part_storage_, String file_name_, ReadSettings read_settings_)
    : part_storage(std::move(part_storage_))
    , file_name(std::move(file_name_))
    , read_settings(std::move(read_settings_))
{
}

RowsSourcesInPart::~RowsSourcesInPart()
{
    if (compressed_out)
        compressed_out->cancel();
    if (out)
        out->cancel();
}

void RowsSourcesInPart::restore(size_t file_size_)
{
    if (out || finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was already written, it cannot be restored");

    file_size = file_size_;
    finalized = true;
}

size_t RowsSourcesInPart::getFileSize() const
{
    if (!finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file is not finalized yet");
    return file_size;
}

WriteBuffer & RowsSourcesInPart::getTemporaryFileForWriting(const String & name)
{
    if (name != FILE_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

    if (out || finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was already requested for writing, there musto be only one writer");

    out = part_storage->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    compressed_out = std::make_unique<CompressedWriteBuffer>(*out);
    return *compressed_out;
}

std::unique_ptr<ReadBuffer> RowsSourcesInPart::getTemporaryFileForReading(const String & name)
{
    if (name != FILE_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

    if (!finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file is not finalized yet");

    /// Reopen the file for each read so that multiple reads can be performed in parallel and there is no need to seek to the beginning.
    return std::make_unique<CompressedReadBufferFromFile>(part_storage->readFile(file_name, read_settings, {}));
}

size_t RowsSourcesInPart::finalizeWriting()
{
    if (!compressed_out)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was not requested for writing");

    compressed_out->finalize();
    const size_t size = compressed_out->count();
    compressed_out.reset();
    out->finalize();
    file_size = out->count();
    out.reset();
    finalized = true;
    return size;
}

}
