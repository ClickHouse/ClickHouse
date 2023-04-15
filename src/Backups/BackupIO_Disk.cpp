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

void BackupReaderDisk::copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                                      WriteMode write_mode, const WriteSettings & write_settings)
{
    if ((write_mode == WriteMode::Rewrite) && (destination_disk->getDataSourceDescription() == getDataSourceDescription()))
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} using {} disk", file_name, toString(destination_disk->getDataSourceDescription().type));
        disk->copyFile(path / file_name, *destination_disk, destination_path, write_settings);
        return;
    }

    /// Fallback to copy through buffers.
    IBackupReader::copyFileToDisk(file_name, size, destination_disk, destination_path, write_mode, write_settings);
}

DataSourceDescription BackupReaderDisk::getDataSourceDescription() const
{
    return disk->getDataSourceDescription();
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

DataSourceDescription BackupWriterDisk::getDataSourceDescription() const
{
    return disk->getDataSourceDescription();
}

void BackupWriterDisk::copyFileFromDisk(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name)
{
    /// IDisk::copyFile() can copy to the same disk only, and it cannot do the throttling.
    if (!has_throttling && (getDataSourceDescription() == src_disk->getDataSourceDescription()))
    {
        /// IDisk::copyFile() can copy a file as a whole only.
        if ((src_offset == 0) && (src_size == src_disk->getFileSize(src_file_name)))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} using {} disk", src_file_name, toString(src_disk->getDataSourceDescription().type));
            auto dest_file_path = path / dest_file_name;
            disk->createDirectories(dest_file_path.parent_path());
            src_disk->copyFile(src_file_name, *disk, dest_file_path);
            return;
        }
    }

    /// Fallback to copy through buffers.
    IBackupWriter::copyFileFromDisk(src_disk, src_file_name, src_offset, src_size, dest_file_name);
}

}
