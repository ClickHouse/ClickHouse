#include <Backups/BackupEntryFromImmutableFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/IDisk.h>
#include <city.h>


namespace DB
{

namespace
{
    /// We mix the checksum calculated for non-encrypted data with IV generated to encrypt the file
    /// to generate kind of a checksum for encrypted data. Of course it differs from the CityHash properly calculated for encrypted data.
    UInt128 combineChecksums(UInt128 checksum1, UInt128 checksum2)
    {
        chassert(std::size(checksum2.items) == 2);
        return CityHash_v1_0_2::CityHash128WithSeed(reinterpret_cast<const char *>(&checksum1), sizeof(checksum1), {checksum2.items[0], checksum2.items[1]});
    }
}

BackupEntryFromImmutableFile::BackupEntryFromImmutableFile(
    const DiskPtr & disk_,
    const String & file_path_,
    bool copy_encrypted_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , file_size(file_size_)
    , checksum(checksum_)
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
    std::lock_guard lock{size_and_checksum_mutex};
    if (!file_size_adjusted)
    {
        if (!file_size)
            file_size = copy_encrypted ? disk->getEncryptedFileSize(file_path) : disk->getFileSize(file_path);
        else if (copy_encrypted)
            file_size = disk->getEncryptedFileSize(*file_size);
        file_size_adjusted = true;
    }
    return *file_size;
}

UInt128 BackupEntryFromImmutableFile::getChecksum(const ReadSettings & read_settings) const
{
    {
        std::lock_guard lock{size_and_checksum_mutex};
        if (checksum_adjusted)
            return *checksum;

        if (checksum)
        {
            if (copy_encrypted)
                checksum = combineChecksums(*checksum, disk->getEncryptedFileIV(file_path));
            checksum_adjusted = true;
            return *checksum;
        }
    }

    auto calculated_checksum = BackupEntryWithChecksumCalculation<IBackupEntry>::getChecksum(read_settings);

    {
        std::lock_guard lock{size_and_checksum_mutex};
        if (!checksum_adjusted)
        {
            checksum = calculated_checksum;
            checksum_adjusted = true;
        }
        return *checksum;
    }
}

std::optional<UInt128> BackupEntryFromImmutableFile::getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const
{
    if (prefix_length == 0)
        return 0;

    if (prefix_length >= getSize())
        return getChecksum(read_settings);

    /// For immutable files we don't use partial checksums.
    return std::nullopt;
}

}
