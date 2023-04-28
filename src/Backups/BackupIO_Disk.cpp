#include <Backups/BackupIO_Disk.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & root_path_, const ContextPtr & context_)
    : BackupReaderDefault(&Poco::Logger::get("BackupReaderDisk"), context_)
    , disk(disk_)
    , root_path(root_path_)
{
}

BackupReaderDisk::~BackupReaderDisk() = default;

bool BackupReaderDisk::fileExists(const String & file_name)
{
    return disk->exists(root_path / file_name);
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
    if ((write_mode == WriteMode::Rewrite) && !encrypted_in_backup)
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} from disk {} to disk {}", path_in_backup, disk->getName(), destination_disk->getName());
        disk->copyFile(root_path / path_in_backup, *destination_disk, destination_path, write_settings);
        return;
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & root_path_, const ContextPtr & context_)
    : BackupWriterDefault(&Poco::Logger::get("BackupWriterDisk"), context_)
    , disk(disk_)
    , root_path(root_path_)
{
}

BackupWriterDisk::~BackupWriterDisk() = default;

bool BackupWriterDisk::fileExists(const String & file_name)
{
    return disk->exists(root_path / file_name);
}

UInt64 BackupWriterDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(root_path / file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterDisk::readFile(const String & file_name, size_t expected_file_size)
{
    return disk->readFile(file_name, read_settings, {}, expected_file_size);
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
    if (disk->isDirectory(root_path) && disk->isDirectoryEmpty(root_path))
        disk->removeDirectory(root_path);
}

void BackupWriterDisk::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        disk->removeFileIfExists(root_path / file_name);
    if (disk->isDirectory(root_path) && disk->isDirectoryEmpty(root_path))
        disk->removeDirectory(root_path);
}

void BackupWriterDisk::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                        bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    if (!copy_encrypted && !start_pos && (length == src_disk->getFileSize(src_path)))
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} from disk {} to disk {}", src_path, src_disk->getName(), disk->getName());
        auto dest_file_path = root_path / path_in_backup;
        disk->createDirectories(dest_file_path.parent_path());
        src_disk->copyFile(src_path, *disk, dest_file_path, write_settings);
        return;
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

}
