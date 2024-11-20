#include <Backups/BackupIO_Disk.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>


namespace DB
{

BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderDisk"))
    , disk(disk_)
    , root_path(root_path_)
    , data_source_description(disk->getDataSourceDescription())
{
}

BackupReaderDisk::~BackupReaderDisk() = default;

bool BackupReaderDisk::fileExists(const String & file_name)
{
    return disk->existsFile(root_path / file_name);
}

UInt64 BackupReaderDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(root_path / file_name);
}

std::unique_ptr<SeekableReadBuffer> BackupReaderDisk::readFile(const String & file_name)
{
    return disk->readFile(root_path / file_name, read_settings);
}

void BackupReaderDisk::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                      DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    /// Use IDisk::copyFile() as a more optimal way to copy a file if it's possible.
    /// However IDisk::copyFile() can't use throttling for reading, and can't copy an encrypted file or do appending.
    bool has_throttling = disk->isRemote() ? static_cast<bool>(read_settings.remote_throttler) : static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling && (write_mode == WriteMode::Rewrite) && !encrypted_in_backup)
    {
        auto destination_data_source_description = destination_disk->getDataSourceDescription();
        if (destination_data_source_description.sameKind(data_source_description) && !data_source_description.is_encrypted)
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} from disk {} to disk {}", path_in_backup, disk->getName(), destination_disk->getName());
            disk->copyFile(root_path / path_in_backup, *destination_disk, destination_path, read_settings, write_settings);
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterDisk"))
    , disk(disk_)
    , root_path(root_path_)
    , data_source_description(disk->getDataSourceDescription())
{
}

BackupWriterDisk::~BackupWriterDisk() = default;

bool BackupWriterDisk::fileExists(const String & file_name)
{
    return disk->existsFile(root_path / file_name);
}

UInt64 BackupWriterDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(root_path / file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterDisk::readFile(const String & file_name, size_t expected_file_size)
{
    return disk->readFile(root_path / file_name, read_settings.adjustBufferSize(expected_file_size));
}

std::unique_ptr<WriteBuffer> BackupWriterDisk::writeFile(const String & file_name)
{
    auto file_path = root_path / file_name;
    disk->createDirectories(file_path.parent_path());
    return disk->writeFile(file_path, write_buffer_size, WriteMode::Rewrite, write_settings);
}

void BackupWriterDisk::removeFile(const String & file_name)
{
    disk->removeFileIfExists(root_path / file_name);
    if (disk->existsDirectory(root_path) && disk->isDirectoryEmpty(root_path))
        disk->removeDirectory(root_path);
}

void BackupWriterDisk::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        disk->removeFileIfExists(root_path / file_name);
    if (disk->existsDirectory(root_path) && disk->isDirectoryEmpty(root_path))
        disk->removeDirectory(root_path);
}

void BackupWriterDisk::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                        bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Use IDisk::copyFile() as a more optimal way to copy a file if it's possible.
    /// However IDisk::copyFile() can't use throttling for reading, and can't copy an encrypted file or copy a part of the file.
    bool has_throttling = src_disk->isRemote() ? static_cast<bool>(read_settings.remote_throttler) : static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling && !start_pos && !copy_encrypted)
    {
        auto source_data_source_description = src_disk->getDataSourceDescription();
        if (source_data_source_description.sameKind(data_source_description) && !source_data_source_description.is_encrypted
            && (length == src_disk->getFileSize(src_path)))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} from disk {} to disk {}", src_path, src_disk->getName(), disk->getName());
            auto dest_file_path = root_path / path_in_backup;
            disk->createDirectories(dest_file_path.parent_path());
            src_disk->copyFile(src_path, *disk, dest_file_path, read_settings, write_settings);
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterDisk::copyFile(const String & destination, const String & source, size_t /*size*/)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);
    auto dest_file_path = root_path / destination;
    auto src_file_path = root_path / source;
    disk->createDirectories(dest_file_path.parent_path());
    disk->copyFile(src_file_path, *disk, dest_file_path, read_settings, write_settings);
}

}
