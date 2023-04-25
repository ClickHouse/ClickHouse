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
    const std::optional<UInt128> & checksum_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , settings(settings_)
    , file_size(data_source_description.is_encrypted ? std::optional<UInt64>{} : file_size_)
    , checksum(data_source_description.is_encrypted ? std::optional<UInt128>{} : checksum_)
{
}

BackupEntryFromImmutableFile::~BackupEntryFromImmutableFile() = default;

UInt64 BackupEntryFromImmutableFile::getSize() const
{
    std::lock_guard lock{get_file_size_mutex};
    if (!file_size)
    {
        if (data_source_description.is_encrypted)
            file_size = disk->getEncryptedFileSize(file_path);
        else
            file_size = disk->getFileSize(file_path);
    }
    return *file_size;
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromImmutableFile::getReadBuffer() const
{
    if (data_source_description.is_encrypted)
        return disk->readEncryptedFile(file_path, settings);
    else
        return disk->readFile(file_path, settings);
}

}
