#include <Backups/BackupEntryFromImmutableFile.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Poco/File.h>
#include <Common/filesystemHelpers.h>


namespace DB
{

BackupEntryFromImmutableFile::BackupEntryFromImmutableFile(
    const DiskPtr & disk_,
    const String & file_path_,
    const ReadSettings & settings_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const std::shared_ptr<TemporaryFileOnDisk> & temporary_file_)
    : disk(disk_)
    , file_path(file_path_)
    , settings(settings_)
    , file_size(file_size_)
    , checksum(checksum_)
    , temporary_file_on_disk(temporary_file_)
{
}

BackupEntryFromImmutableFile::~BackupEntryFromImmutableFile() = default;

UInt64 BackupEntryFromImmutableFile::getSize() const
{
    std::lock_guard lock{get_file_size_mutex};
    if (!file_size)
        file_size = disk->getFileSize(file_path);
    return *file_size;
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromImmutableFile::getReadBuffer() const
{
    return disk->readFile(file_path, settings);
}


DataSourceDescription BackupEntryFromImmutableFile::getDataSourceDescription() const
{
    return disk->getDataSourceDescription();
}

String BackupEntryFromImmutableFile::getFilePath() const
{
    return file_path;
}

}
