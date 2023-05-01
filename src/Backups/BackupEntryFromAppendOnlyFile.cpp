#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Disks/DiskEncrypted.h>
#include <IO/LimitSeekableReadBuffer.h>


namespace DB
{

namespace
{
    UInt64 calculateSize(const DiskPtr & disk, const String & file_path, const std::optional<UInt64> & file_size, bool disk_is_encrypted)
    {
        if (file_size)
        {
            if (disk_is_encrypted)
                return DiskEncrypted::convertFileSizeToEncryptedFileSize(*file_size);
            else
                return *file_size;
        }
        else
        {
            if (disk_is_encrypted)
                return disk->getEncryptedFileSize(file_path);
            else
                return disk->getFileSize(file_path);
        }
    }
}

BackupEntryFromAppendOnlyFile::BackupEntryFromAppendOnlyFile(
    const DiskPtr & disk_, const String & file_path_, const ReadSettings & settings_, const std::optional<UInt64> & file_size_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , settings(settings_)
    , size(calculateSize(disk_, file_path_, file_size_, data_source_description.is_encrypted))
{
}

BackupEntryFromAppendOnlyFile::~BackupEntryFromAppendOnlyFile() = default;

std::unique_ptr<SeekableReadBuffer> BackupEntryFromAppendOnlyFile::getReadBuffer() const
{
    std::unique_ptr<SeekableReadBuffer> buf;
    if (data_source_description.is_encrypted)
        buf = disk->readEncryptedFile(file_path, settings);
    else
        buf = disk->readFile(file_path, settings);
    return std::make_unique<LimitSeekableReadBuffer>(std::move(buf), 0, size);
}

}
