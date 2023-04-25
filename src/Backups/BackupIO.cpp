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

void IBackupReader::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                   DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, const WriteSettings & write_settings)
{
    LOG_TRACE(log, "Copying file {} to disk {} through buffers", path_in_backup, destination_disk->getName());
    auto read_buffer = readFile(path_in_backup);
    auto buf_size = std::min<size_t>(file_size, DBMS_DEFAULT_BUFFER_SIZE);
    std::unique_ptr<WriteBuffer> write_buffer;
    if (encrypted_in_backup)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, write_settings);
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, write_settings);
    copyData(*read_buffer, *write_buffer, file_size);
    write_buffer->finalize();
}

IBackupWriter::IBackupWriter(const ContextPtr & context_, Poco::Logger * log_)
    : log(log_), read_settings(context_->getBackupReadSettings())
{
}

void IBackupWriter::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    auto read_buffer = create_read_buffer();
    if (start_pos)
        read_buffer->seek(start_pos, SEEK_SET);
    auto write_buffer = writeFile(path_in_backup);
    copyData(*read_buffer, *write_buffer, length);
    write_buffer->finalize();
}

void IBackupWriter::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                     bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    LOG_TRACE(log, "Copying file {} from disk {} through buffers", src_path, src_disk->getName());
    auto create_read_buffer = [this, src_disk, src_path, copy_encrypted]
    {
        if (copy_encrypted)
            return src_disk->readEncryptedFile(src_path, read_settings);
        else
            return src_disk->readFile(src_path, read_settings);
    };
    copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
}

}
