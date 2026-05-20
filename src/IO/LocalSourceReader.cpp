#include <IO/LocalSourceReader.h>
#include <IO/ReadSettings.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    /// Local files use pread; createReadBufferFromFileBase doesn't expose an
    /// external-buffer flag at this level — the executor's set() still works.
    return createReadBufferFromFileBase(object.remote_path, ReadSettings{});
}

}
