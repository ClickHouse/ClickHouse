#include <IO/LocalSourceReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}, size={}", object.remote_path,
        object.bytes_size == StoredObject::UnknownSize ? "unknown" : std::to_string(object.bytes_size));
    /// Preserve the open-time errors of empty files rather than returning `ReadBufferFromEmptyFile`.
    std::optional<size_t> file_size;
    if (object.bytes_size != StoredObject::UnknownSize && object.bytes_size != 0)
        file_size = object.bytes_size;

    return createReadBufferFromFileBase(
        object.remote_path, read_settings, read_hint, file_size);
}

}
