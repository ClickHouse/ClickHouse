#include <IO/LocalSourceReader.h>
#include <IO/ReadSettings.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object, bool /* use_external_buffer */)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    /// Local files use pread — external buffer is not applicable.
    return createReadBufferFromFileBase(object.remote_path, ReadSettings{});
}

}
