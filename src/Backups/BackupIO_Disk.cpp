#include <Backups/BackupIO_Disk.h>
#include <Common/Exception.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <base/logger_useful.h>

namespace fs = std::filesystem;


namespace DB
{
BackupReaderDisk::BackupReaderDisk(const DiskPtr & disk_, const String & path_) : disk(disk_), path(path_)
{
}

BackupReaderDisk::~BackupReaderDisk() = default;

bool BackupReaderDisk::fileExists(const String & file_name)
{
    return disk->exists(path / file_name);
}

size_t BackupReaderDisk::getFileSize(const String & file_name)
{
    return disk->getFileSize(path / file_name);
}

std::unique_ptr<SeekableReadBuffer> BackupReaderDisk::readFile(const String & file_name)
{
    return disk->readFile(path / file_name);
}

BackupWriterDisk::BackupWriterDisk(const DiskPtr & disk_, const String & path_) : disk(disk_), path(path_)
{
}

BackupWriterDisk::~BackupWriterDisk() = default;

bool BackupWriterDisk::fileExists(const String & file_name)
{
    return disk->exists(path / file_name);
}

std::unique_ptr<WriteBuffer> BackupWriterDisk::writeFile(const String & file_name)
{
    auto file_path = path / file_name;
    disk->createDirectories(file_path.parent_path());
    return disk->writeFile(file_path);
}

void BackupWriterDisk::removeFilesAfterFailure(const Strings & file_names)
{
    try
    {
        for (const auto & file_name : file_names)
            disk->removeFileIfExists(path / file_name);
        if (disk->isDirectory(path) && disk->isDirectoryEmpty(path))
            disk->removeDirectory(path);
    }
    catch (...)
    {
        LOG_WARNING(&Poco::Logger::get("BackupWriterDisk"), "RemoveFilesAfterFailure: {}", getCurrentExceptionMessage(false));
    }
}

}
