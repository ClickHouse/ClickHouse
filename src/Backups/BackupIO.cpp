#include <Backups/BackupIO.h>

#include <IO/copyData.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{

IBackupReader::IBackupReader(Poco::Logger * log_) : log(log_)
{
}

void IBackupReader::copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                                   WriteMode write_mode, const WriteSettings & write_settings)
{
    LOG_TRACE(log, "Copying file {} through buffers", file_name);
    auto read_buffer = readFile(file_name);
    auto write_buffer = destination_disk->writeFile(destination_path, std::min<size_t>(size, DBMS_DEFAULT_BUFFER_SIZE), write_mode, write_settings);
    copyData(*read_buffer, *write_buffer, size);
    write_buffer->finalize();
}

IBackupWriter::IBackupWriter(const ContextPtr & context_, Poco::Logger * log_)
    : log(log_), read_settings(context_->getBackupReadSettings())
{
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

void IBackupWriter::copyFileFromDisk(
    DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name)
{
    LOG_TRACE(log, "Copying file {} through buffers", src_file_name);
    auto create_read_buffer = [this, src_disk, src_file_name] { return src_disk->readFile(src_file_name, read_settings); };
    copyDataToFile(create_read_buffer, src_offset, src_size, dest_file_name);
}

}
