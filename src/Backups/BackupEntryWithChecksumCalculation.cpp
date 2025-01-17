#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY;
    extern const int LOGICAL_ERROR;
}

UInt128 BackupEntryWithChecksumCalculation::getChecksum(const ReadSettings & read_settings) const
{
    if (calculated_checksum)
        return *calculated_checksum;

    UInt64 size = getSize();
    if (size == 0)
        calculated_checksum = 0;
    else
        calculated_checksum = calculateChecksum(size, {}, {}, read_settings);

    return *calculated_checksum;
}


std::optional<UInt128> BackupEntryWithChecksumCalculation::getPartialChecksum(UInt64 limit, const ReadSettings & read_settings) const
{
    if (limit == 0)
        return 0;

    UInt64 size = getSize();
    if (limit >= size)
        return getChecksum(read_settings);

    if (!isPartialChecksumAllowed())
        return {};

    if (calculated_checksum)
        return calculateChecksum(limit, {}, {}, read_settings);

    UInt128 full_checksum;
    UInt128 partial_checksum = calculateChecksum(limit, size, &full_checksum, read_settings);
    calculated_checksum = full_checksum;
    return partial_checksum;
}


UInt128 BackupEntryWithChecksumCalculation::calculateChecksum(
    UInt64 limit,
    std::optional<UInt64> second_limit, UInt128 * second_checksum,
    const ReadSettings & read_settings) const
{
    if (canCalculateChecksumFromBlobPaths())
        calculateChecksumFromBlobPaths(limit, second_limit, second_checksum);

    return calculateChecksumFromReadBuffer(limit, second_limit, second_checksum, read_settings);
}


UInt128 BackupEntryWithChecksumCalculation::calculateChecksumFromReadBuffer(
    UInt64 limit,
    std::optional<UInt64> second_limit, UInt128 * second_checksum,
    const ReadSettings & read_settings) const
{
    UInt64 size = getSize();
    limit = std::min(limit, size);
    if (second_limit)
        second_limit = std::min(*second_limit, size);

    std::unique_ptr<SeekableReadBuffer> read_buffer;
    std::unique_ptr<HashingReadBuffer> hashing_read_buffer;

    auto start_reading = [&]
    {
        if (!hashing_read_buffer)
        {
            read_buffer = getReadBuffer(read_settings.adjustBufferSize(second_limit.value_or(limit)));
            hashing_read_buffer = std::make_unique<HashingReadBuffer>(*read_buffer);
        }
    };

    UInt128 checksum;

    if (limit == 0)
    {
        checksum = 0;
    }
    else if (limit == size || isPartialChecksumAllowed())
    {
        start_reading();
        hashing_read_buffer->ignore(limit);
        checksum = hashing_read_buffer->getHash();
    }
    else
    {
        throw Exception(ErrorCodes::CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY,
                "File {} ({} bytes): couldn't calculate checksum for bytes [0..{}) for backup (partial checksums are not allowed)",
                getFilePath(), size, limit);
    }

    if (second_limit)
    {
        chassert(second_checksum);

        if (*second_limit < limit)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "First limit {} must not be greater than second limit {} while calculating partial checksums (file: {})",
                            limit, *second_limit, getFilePath());
        }

        if (*second_limit == limit)
        {
            *second_checksum = checksum;
        }
        else if (*second_limit == size || isPartialChecksumAllowed())
        {
            start_reading();
            hashing_read_buffer->ignore(*second_limit - limit);
            *second_checksum = hashing_read_buffer->getHash();
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY,
                    "File {} ({} bytes): couldn't calculate checksum for bytes [0..{}) for backup (partial checksums are not allowed)",
                    getFilePath(), size, *second_limit);
        }
    }

    return checksum;
}


bool BackupEntryWithChecksumCalculation::canCalculateChecksumFromBlobPaths() const
{
    return !getFilePath().empty() && getDisk() && getDisk()->areBlobPathsRandom();
}


UInt128 BackupEntryWithChecksumCalculation::calculateChecksumFromBlobPaths(
    UInt64 limit,
    std::optional<UInt64> second_limit, UInt128 * second_checksum) const
{
    UInt64 size = getSize();
    limit = std::min(limit, size);
    if (second_limit)
        second_limit = std::min(*second_limit, size);

    UInt128 hash = 0;
    size_t index = 0;

    auto add_to_hash = [&](const StoredObject & obj)
    {
        if (index == 0)
            hash = CityHash128(obj.remote_path.data(), obj.remote_path.length());
        else
            hash = CityHash128WithSeed(obj.remote_path.data(), obj.remote_path.length(), hash);
    };

    StoredObjects stored_objects = getDisk()->getStorageObjects(getFilePath());

    size_t offset = 0;

    while (index < stored_objects.size() && offset < limit)
    {
        add_to_hash(stored_objects[index]);
        offset += stored_objects[index].bytes_size;
        ++index;
    }

    if (offset != limit)
    {
        throw Exception(ErrorCodes::CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY,
            "File {} ({} bytes): couldn't calculate checksum for bytes [0..{}) for backup, blobs have unexpected sizes: {}",
            getFilePath(), size, limit, remotePathsAndBytesSizesToString(stored_objects));
    }

    UInt128 checksum = hash;

    if (second_limit)
    {
        while (index < stored_objects.size() && offset < *second_limit)
        {
            add_to_hash(stored_objects[index]);
            offset += stored_objects[index].bytes_size;
            ++index;
        }

        if (offset != *second_limit)
        {
            throw Exception(ErrorCodes::CANNOT_CALCULATE_CHECKSUM_FOR_BACKUP_ENTRY,
                "File {} ({} bytes): couldn't calculate checksum for bytes [0..{}) for backup, blobs have unexpected sizes: {}",
                getFilePath(), size, *second_limit, remotePathsAndBytesSizesToString(stored_objects));
        }

        *second_checksum = hash;
    }

    return checksum;
}

}
