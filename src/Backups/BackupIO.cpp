#include <Backups/BackupIO.h>

#include <IO/copyData.h>
#include <IO/WriteBuffer.h>
#include <IO/SeekableReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void IBackupWriter::copyFileThroughBuffer(std::unique_ptr<SeekableReadBuffer> && source, const String & file_name)
{
    auto write_buffer = writeFile(file_name);
    copyData(*source, *write_buffer);
    write_buffer->finalize();
}

void IBackupWriter::copyFileNative(DiskPtr /* from_disk */, const String & /* file_name_from */, const String & /* file_name_to */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native copy not implemented for backup writer");
}

}
