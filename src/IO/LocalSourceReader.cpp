#include <IO/LocalSourceReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> LocalSourceReader::open(const StoredObject & object)
{
    LOG_TRACE(log, "open: file={}", object.remote_path);
    /// Pass the file size so createReadBufferFromFileBase applies the direct-IO and
    /// mmap thresholds (min_bytes_to_use_direct_io etc.) the same way the legacy
    /// local reader does - the decision keys off the size of the read.
    return createReadBufferFromFileBase(
        object.remote_path, read_settings, /*read_hint=*/{}, /*file_size=*/object.bytes_size);
}

}
