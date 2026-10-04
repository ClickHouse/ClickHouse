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

std::unique_ptr<ReadBufferFromFileBase> ObjectStorageSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: object={}, size={}", object.remote_path,
        object.bytes_size == StoredObject::UnknownSize ? "unknown" : std::to_string(object.bytes_size));
    return storage->readObject(object, read_settings, /*read_hint=*/{}, /*use_external_buffer=*/true);
}

}
