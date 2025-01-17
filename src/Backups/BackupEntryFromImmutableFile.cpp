#include <Backups/BackupEntryFromImmutableFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/IDisk.h>
#include <city.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY;
}

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
    , passed_file_size(file_size_)
    , passed_checksum(checksum_)
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
    if (calculated_size)
        return *calculated_size;

    calculated_size = calculateSize();
    return *calculated_size;
}

UInt64 BackupEntryFromImmutableFile::calculateSize() const
{
    if (copy_encrypted)
        return passed_file_size ? disk->getEncryptedFileSize(*passed_file_size) : disk->getEncryptedFileSize(file_path);

    if (passed_file_size)
        return *passed_file_size;

    return disk->getFileSize(file_path);
}

UInt128 BackupEntryFromImmutableFile::calculateChecksum(UInt64 limit, std::optional<UInt64> second_limit, UInt128 * second_checksum, const ReadSettings & read_settings) const
{
    if (limit == 0)
        return 0;

    UInt64 size = getSize();
    limit = std::min(limit, size);

    if (limit < size)
    {
        throw Exception(ErrorCodes::CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY,
                "File {} ({} bytes): couldn't calculate checksum for bytes [0..{}) for backup (partial checksums are not allowed)",
                getFilePath(), size, limit);
    }

    UInt128 checksum;
    if (passed_checksum && !copy_encrypted)
        checksum = *passed_checksum;
    else if (canCalculateChecksumFromBlobPaths())
        checksum = calculateChecksumFromBlobPaths(limit, {}, {});
    else if (passed_checksum && copy_encrypted)
        checksum = combineChecksums(*passed_checksum, disk->getEncryptedFileIV(file_path));
    else
        checksum = calculateChecksumFromReadBuffer(limit, {}, {}, read_settings);

    if (second_limit)
    {
        chassert(second_checksum);
        if (std::min(*second_limit, size) == limit)
            *second_checksum = checksum;
        else
            *second_checksum = calculateChecksum(*second_limit, {}, {}, read_settings);
    }

    return checksum;
}

}
