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

std::unique_ptr<ReadBufferFromFileBase> ObjectStorageSourceReader::open(const StoredObject & object, bool use_external_buffer)
{
    LOG_TRACE(log, "open: object={}, use_external_buffer={}", object.remote_path, use_external_buffer);
    return storage->readObject(object, read_settings, /*read_hint=*/{}, use_external_buffer);
}

}
