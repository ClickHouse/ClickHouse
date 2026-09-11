#include <IO/ObjectStorageSourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>

namespace DB
{

ObjectStorageSourceReader::ObjectStorageSourceReader(
    ObjectStoragePtr storage_,
    const ReadSettings & read_settings_,
    std::function<void()> cancellation_hook_)
    : storage(std::move(storage_))
    , read_settings(read_settings_)
    , cancellation_hook(std::move(cancellation_hook_))
{
}

std::unique_ptr<ReadBufferFromFileBase> ObjectStorageSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: object={}, size={}", object.remote_path,
        object.bytes_size == StoredObject::UnknownSize ? "unknown" : std::to_string(object.bytes_size));
    if (cancellation_hook)
        return storage->readObjectForCopy(object, read_settings, cancellation_hook, /*read_hint=*/{}, /*use_external_buffer=*/true);
    return storage->readObject(object, read_settings, /*read_hint=*/{}, /*use_external_buffer=*/true);
}

}
