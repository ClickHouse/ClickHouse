#include <Backups/BackupEntryFromImmutableFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/IDisk.h>


namespace DB
{

BackupEntryFromImmutableFile::BackupEntryFromImmutableFile(
    const DiskPtr & disk_,
    const String & file_path_,
    bool copy_encrypted_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    bool allow_checksum_from_remote_path_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , passed_file_size(file_size_)
    , passed_checksum(checksum_)
    , allow_checksum_from_remote_path(allow_checksum_from_remote_path_)
{
}

BackupEntryFromImmutableFile::~BackupEntryFromImmutableFile() = default;

std::unique_ptr<SeekableReadBuffer> BackupEntryFromImmutableFile::getReadBuffer(const ReadSettings & read_settings) const
{
    if (copy_encrypted)
        return disk->readEncryptedFile(file_path, read_settings);
    return disk->readFile(file_path, read_settings);
}

UInt64 BackupEntryFromImmutableFile::getSize() const
{
    {
        std::lock_guard lock{mutex};
        if (calculated_size)
            return *calculated_size;
    }

    UInt64 size = calculateSize();

    {
        std::lock_guard lock{mutex};
        calculated_size = size;
    }

    return size;
}

UInt64 BackupEntryFromImmutableFile::calculateSize() const
{
    if (copy_encrypted)
        return passed_file_size ? disk->getEncryptedFileSize(*passed_file_size) : disk->getEncryptedFileSize(file_path);

    if (passed_file_size)
        return *passed_file_size;

    return disk->getFileSize(file_path);
}

}
