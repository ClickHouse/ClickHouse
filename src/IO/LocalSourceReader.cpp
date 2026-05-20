#include <IO/LocalSourceReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    /// Pass caller-configured ReadSettings (local_fs_method, direct_io_threshold,
    /// local_fs_buffer_size, throttlers, etc.); createReadBufferFromFileBase
    /// doesn't expose an external-buffer flag — the executor's set() still works.
    return createReadBufferFromFileBase(object.remote_path, read_settings);
}

}
