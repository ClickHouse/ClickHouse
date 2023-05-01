#include <Backups/BackupEntryFromImmutableFile.h>
#include <Disks/IDisk.h>
#include <Disks/DiskEncrypted.h>


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
    , file_size(file_size_)
    , checksum(checksum_)
{
}

BackupEntryFromImmutableFile::~BackupEntryFromImmutableFile() = default;

std::unique_ptr<SeekableReadBuffer> BackupEntryFromImmutableFile::getReadBuffer() const
{
    if (data_source_description.is_encrypted)
        return disk->readEncryptedFile(file_path, settings);
    else
        return disk->readFile(file_path, settings);
}

UInt64 BackupEntryFromImmutableFile::getSize() const
{
    std::lock_guard lock{size_and_checksum_mutex};
    if (!file_size_adjusted)
    {
        if (!file_size)
            file_size = disk->getFileSize(file_path);
        if (data_source_description.is_encrypted)
            *file_size = DiskEncrypted::convertFileSizeToEncryptedFileSize(*file_size);
        file_size_adjusted = true;
    }
    return *file_size;
}

UInt128 BackupEntryFromImmutableFile::getChecksum() const
{
    std::lock_guard lock{size_and_checksum_mutex};
    if (!checksum_adjusted)
    {
        /// TODO: We should not just ignore `checksum` if `data_source_description.is_encrypted == true`, we should use it somehow.
        if (!checksum || data_source_description.is_encrypted)
            checksum = BackupEntryWithChecksumCalculation<IBackupEntry>::getChecksum();
        checksum_adjusted = true;
    }
    return *checksum;
}

std::optional<UInt128> BackupEntryFromImmutableFile::getPartialChecksum(size_t prefix_length) const
{
    if (prefix_length == 0)
        return 0;

    if (prefix_length >= getSize())
        return getChecksum();

    /// For immutable files we don't use partial checksums.
    return std::nullopt;
}

}
