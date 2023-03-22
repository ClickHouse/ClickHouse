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

BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & path_)
    : disk(disk_), path(path_), log(&Poco::Logger::get("BackupReaderDisk"))
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
    if (write_mode == WriteMode::Rewrite)
    {
        LOG_TRACE(log, "Copying {}/{} from disk {} to {} by the disk", path, file_name, disk->getName(), destination_disk->getName());
        disk->copyFile(path / file_name, *destination_disk, destination_path, write_settings);
        return;
    }

    LOG_TRACE(log, "Copying {}/{} from disk {} to {} through buffers", path, file_name, disk->getName(), destination_disk->getName());
    IBackupReader::copyFileToDisk(file_name, size, destination_disk, destination_path, write_mode, write_settings);
}


BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & path_) : disk(disk_), path(path_)
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

DataSourceDescription BackupReaderDisk::getDataSourceDescription() const
{
    return disk->getDataSourceDescription();
}

bool BackupWriterDisk::supportNativeCopy(DataSourceDescription data_source_description) const
{
    return data_source_description == disk->getDataSourceDescription();
}

void BackupWriterDisk::copyFileNative(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name)
{
    if (!src_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot natively copy data to disk without source disk");

    if ((src_offset != 0) || (src_size != src_disk->getFileSize(src_file_name)))
    {
        auto create_read_buffer = [src_disk, src_file_name] { return src_disk->readFile(src_file_name); };
        copyDataToFile(create_read_buffer, src_offset, src_size, dest_file_name);
        return;
    }

    auto file_path = path / dest_file_name;
    disk->createDirectories(file_path.parent_path());
    src_disk->copyFile(src_file_name, *disk, file_path);
}

}
