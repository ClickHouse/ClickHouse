#include <IO/LocalSourceReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    /// Pass caller-configured ReadSettings (local_fs_method, direct_io_threshold,
    /// throttlers, etc.). ReaderExecutor copies the bytes out of the returned
    /// buffer via buf.read(block, chunk), which works for both synchronous
    /// (pread) and asynchronous (pread_threadpool / io_uring) read methods —
    /// the latter ignore set()'s external-buffer pointer and read into their
    /// own allocation, so explicit copy is the only correct option.
    return createReadBufferFromFileBase(object.remote_path, read_settings);
}

}
