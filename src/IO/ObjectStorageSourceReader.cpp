#include <IO/ObjectStorageSourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>

namespace DB
{

ObjectStorageSourceReader::ObjectStorageSourceReader(
    ObjectStoragePtr storage_,
    const ReadSettings & read_settings_)
    : storage(std::move(storage_))
    , read_settings(read_settings_)
{
}

size_t ObjectStorageSourceReader::read(
    const StoredObject & object,
    size_t offset, size_t size,
    char * buffer)
{
    LOG_TRACE(log, "read: object={}, offset={}, size={}", object.remote_path, offset, size);

    auto buf = storage->readObject(object, read_settings);

    /// Prefer readBigAt for stateless range reads (S3, Azure support this).
    if (buf->supportsReadAt())
    {
        size_t bytes_read = buf->readBigAt(buffer, size, offset, {});
        LOG_TRACE(log, "read: readBigAt got {} bytes from {}", bytes_read, object.remote_path);
        return bytes_read;
    }

    /// Fallback: seek + read for storages that don't support readBigAt.
    buf->seek(offset, SEEK_SET);

    size_t total_read = 0;
    while (total_read < size)
    {
        size_t remaining = size - total_read;
        size_t bytes = buf->read(buffer + total_read, remaining);
        if (bytes == 0)
            break;
        total_read += bytes;
    }

    LOG_TRACE(log, "read: seek+read got {} bytes from {}", total_read, object.remote_path);
    return total_read;
}

}
