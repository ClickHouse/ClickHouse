#include <Backups/BackupIO_Disk.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & path_)
    : IBackupReader(&Poco::Logger::get("BackupReaderDisk")), disk(disk_), path(path_)
{
}

BackupReaderDisk::~BackupReaderDisk() = default;

bool BackupReaderDisk::fileExists(const String & file_name)
{
    return disk->exists(path / file_name);
}

UInt64 BackupReaderDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(path / file_name);
}

std::unique_ptr<SeekableReadBuffer> BackupReaderDisk::readFile(const String & file_name)
{
    return disk->readFile(path / file_name);
}

void BackupReaderDisk::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                      DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, const WriteSettings & write_settings)
{
    if ((write_mode == WriteMode::Rewrite) && !encrypted_in_backup)
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} from disk {} to disk {}", path_in_backup, disk->getName(), destination_disk->getName());
        disk->copyFile(path / path_in_backup, *destination_disk, destination_path, write_settings);
        return;
    }

    /// Fallback to copy through buffers.
    IBackupReader::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode, write_settings);
}


BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & path_, const ContextPtr & context_)
    : IBackupWriter(context_, &Poco::Logger::get("BackupWriterDisk"))
    , disk(disk_)
    , path(path_)
    , has_throttling(static_cast<bool>(context_->getBackupsThrottler()))
{
}

BackupWriterDisk::~BackupWriterDisk() = default;

bool BackupWriterDisk::fileExists(const String & file_name)
{
    return disk->exists(path / file_name);
}

UInt64 BackupWriterDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(path / file_name);
}

bool BackupWriterDisk::fileContentsEqual(const String & file_name, const String & expected_file_contents)
{
    if (!disk->exists(path / file_name))
        return false;

    try
    {
        auto in = disk->readFile(path / file_name);
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

std::unique_ptr<WriteBuffer> BackupWriterDisk::writeFile(const String & file_name)
{
    auto file_path = path / file_name;
    disk->createDirectories(file_path.parent_path());
    return disk->writeFile(file_path);
}

void BackupWriterDisk::removeFile(const String & file_name)
{
    disk->removeFileIfExists(path / file_name);
    if (disk->isDirectory(path) && disk->isDirectoryEmpty(path))
        disk->removeDirectory(path);
}

void BackupWriterDisk::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        disk->removeFileIfExists(path / file_name);
    if (disk->isDirectory(path) && disk->isDirectoryEmpty(path))
        disk->removeDirectory(path);
}

void BackupWriterDisk::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                        bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    if (!copy_encrypted && !start_pos && (length == src_disk->getFileSize(src_path)))
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} from disk {} to disk {}", src_path, src_disk->getName(), disk->getName());
        auto dest_file_path = path / path_in_backup;
        disk->createDirectories(dest_file_path.parent_path());
        src_disk->copyFile(src_path, *disk, dest_file_path);
        return;
    }

    /// Fallback to copy through buffers.
    IBackupWriter::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

}
