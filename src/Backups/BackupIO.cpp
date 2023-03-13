#include <Backups/BackupIO.h>

#include <IO/copyData.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void IBackupReader::copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path, WriteMode mode)
{
    if (supportNativeCopy(destination_disk->getDataSourceDescription(), mode))
    {
        LOG_TRACE(getLogger(), "Copying {} using native copy", file_name);
        copyFileToDiskNative(file_name, size, destination_disk, destination_path, mode);
    }
    else
    {
        LOG_TRACE(getLogger(), "Copying {} through buffers", file_name);
        auto read_buffer = readFile(file_name);
        auto write_buffer = destination_disk->writeFile(destination_path, DBMS_DEFAULT_BUFFER_SIZE, mode);
        copyData(*read_buffer, *write_buffer, size);
        write_buffer->finalize();
    }
}

void IBackupWriter::copyDataToFile(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name)
{
    auto read_buffer = create_read_buffer();
    if (offset)
        read_buffer->seek(offset, SEEK_SET);
    auto write_buffer = writeFile(dest_file_name);
    copyData(*read_buffer, *write_buffer, size);
    write_buffer->finalize();
}

void IBackupWriter::copyFileNative(
    DiskPtr /* src_disk */, const String & /* src_file_name */, UInt64 /* src_offset */, UInt64 /* src_size */, const String & /* dest_file_name */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native copy not implemented for backup writer");
}
}
