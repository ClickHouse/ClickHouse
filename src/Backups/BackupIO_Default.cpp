#include <Backups/BackupIO_Default.h>

#include <Disks/IDisk.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>


namespace DB
{

BackupReaderDefault::BackupReaderDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_)
    : log(log_)
    , read_settings(read_settings_)
    , write_settings(write_settings_)
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

void BackupReaderDefault::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                         DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    LOG_TRACE(log, "Copying file {} to disk {} through buffers", path_in_backup, destination_disk->getName());

    auto read_buffer = readFile(path_in_backup);

    std::unique_ptr<WriteBuffer> write_buffer;
    auto buf_size = std::min(file_size, write_buffer_size);
    if (encrypted_in_backup)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, write_settings);
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, write_settings);

    copyData(*read_buffer, *write_buffer, file_size);
    write_buffer->finalize();
}

BackupWriterDefault::BackupWriterDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_)
    : log(log_)
    , read_settings(read_settings_)
    , write_settings(write_settings_)
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

bool BackupWriterDefault::fileContentsEqual(const String & file_name, const String & expected_file_contents)
{
    if (!fileExists(file_name))
        return false;

    try
    {
        auto in = readFile(file_name, expected_file_contents.size());
        String actual_file_contents(expected_file_contents.size(), ' ');
        return (in->read(actual_file_contents.data(), actual_file_contents.size()) == actual_file_contents.size())
            && (actual_file_contents == expected_file_contents) && in->eof();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

void BackupWriterDefault::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    auto read_buffer = create_read_buffer();

    if (start_pos)
        read_buffer->seek(start_pos, SEEK_SET);

    auto write_buffer = writeFile(path_in_backup);

    copyData(*read_buffer, *write_buffer, length);
    write_buffer->finalize();
}

void BackupWriterDefault::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                           bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    LOG_TRACE(log, "Copying file {} from disk {} through buffers", src_path, src_disk->getName());

    auto create_read_buffer = [src_disk, src_path, copy_encrypted, settings = read_settings.adjustBufferSize(start_pos + length)]
    {
        if (copy_encrypted)
            return src_disk->readEncryptedFile(src_path, settings);
        return src_disk->readFile(src_path, settings);
    };

    copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
}
}
