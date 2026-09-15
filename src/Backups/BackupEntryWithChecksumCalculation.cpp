#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <IO/HashingReadBuffer.h>


namespace ProfileEvents
{
    extern const Event BackupReadLocalFilesToCalculateChecksums;
    extern const Event BackupReadLocalBytesToCalculateChecksums;
    extern const Event BackupReadRemoteFilesToCalculateChecksums;
    extern const Event BackupReadRemoteBytesToCalculateChecksums;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int EXPECTED_END_OF_FILE;
    extern const int UNEXPECTED_END_OF_FILE;
}

namespace
{
    /// We mix the checksum calculated for non-encrypted data with IV generated to encrypt the file
    /// to generate kind of a checksum for encrypted data. Of course it differs from the CityHash properly calculated for encrypted data.
    UInt128 combineChecksums(UInt128 checksum1, UInt128 second_checksum)
    {
        chassert(std::size(second_checksum.items) == 2);
        return CityHash_v1_0_2::CityHash128WithSeed(reinterpret_cast<const char *>(&checksum1), sizeof(checksum1), {second_checksum.items[0], second_checksum.items[1]});
    }
}

UInt128 BackupEntryWithChecksumCalculation::getChecksum(const ReadSettings & read_settings) const
{
    {
        std::lock_guard lock{mutex};
        if (calculated_checksum)
            return *calculated_checksum;
    }

    auto full_checksum = calculateChecksum(getSize(), read_settings);
    chassert(full_checksum);

    {
        std::lock_guard lock{mutex};
        calculated_checksum = full_checksum.value();
    }

    return full_checksum.value();
}


std::optional<UInt128> BackupEntryWithChecksumCalculation::getPartialChecksum(UInt64 limit, const ReadSettings & read_settings) const
{
    UInt64 size = getSize();
    if (limit >= size)
        return getChecksum(read_settings);

    bool has_calculated_full_checksum;

    {
        std::lock_guard lock{mutex};
        has_calculated_full_checksum = calculated_checksum.has_value();
    }

    if (has_calculated_full_checksum)
        return calculateChecksum(limit, read_settings);

    auto [partial_checksum, full_checksum] = calculateOneOrTwoChecksums(limit, size, read_settings);
    chassert(full_checksum);

    {
        std::lock_guard lock{mutex};
        calculated_checksum = full_checksum.value();
    }

    return partial_checksum;
}


std::optional<UInt128> BackupEntryWithChecksumCalculation::calculateChecksum(UInt64 limit, const ReadSettings & read_settings) const
{
    return calculateOneOrTwoChecksums(limit, {}, read_settings).first;
}


BackupEntryWithChecksumCalculation::ChecksumCalculationMethod BackupEntryWithChecksumCalculation::chooseChecksumCalculationMethod() const
{
    UInt64 size = getSize();

    ChecksumCalculationMethod method;
    if (size == 0)
    {
        method = ChecksumCalculationMethod::EmptyZero;
    }
    else if (hasPrecalculatedChecksum() && !isEncryptedByDisk())
    {
        method = ChecksumCalculationMethod::Precalculated;
    }
    else if (canCalculateChecksumFromRemotePath())
    {
        method = ChecksumCalculationMethod::FromRemotePath;
    }
    else if (hasPrecalculatedChecksum() && isEncryptedByDisk())
    {
        method = ChecksumCalculationMethod::PrecalculatedCombinedWithEncryptionIV;
    }
    else
    {
        method = ChecksumCalculationMethod::FromReading;
    }

    return method;
}


bool BackupEntryWithChecksumCalculation::hasPrecalculatedChecksum() const
{
    return getPrecalculatedChecksum().has_value();
}


bool BackupEntryWithChecksumCalculation::canCalculateChecksumFromRemotePath() const
{
    return isChecksumFromRemotePathAllowed() && !getFilePath().empty() && getDisk() && getDisk()->areBlobPathsRandom();
}


std::pair<std::optional<UInt128>, std::optional<UInt128>> BackupEntryWithChecksumCalculation::calculateOneOrTwoChecksums(
    UInt64 limit, std::optional<UInt64> second_limit, const ReadSettings & read_settings) const
{
    switch (chooseChecksumCalculationMethod())
    {
        case ChecksumCalculationMethod::EmptyZero:
            return {0, 0};

        case ChecksumCalculationMethod::Precalculated:
            return getPrecalculatedChecksumIfFull(limit, second_limit);

        case ChecksumCalculationMethod::PrecalculatedCombinedWithEncryptionIV:
            return combinePrecalculatedChecksumWithEncryptionIV(limit, second_limit);

        case ChecksumCalculationMethod::FromReading:
            return calculateChecksumFromReading(limit, second_limit, read_settings);

        case ChecksumCalculationMethod::FromRemotePath:
            return calculateChecksumFromRemotePath(limit, second_limit);
    }
    UNREACHABLE();
}


std::pair<std::optional<UInt128>, std::optional<UInt128>>
BackupEntryWithChecksumCalculation::getPrecalculatedChecksumIfFull(UInt64 limit, std::optional<UInt64> second_limit) const
{
    UInt64 size = getSize();
    limit = std::min(limit, size);
    if (second_limit)
        second_limit = std::min(*second_limit, size);

    std::optional<UInt128> checksum;
    if (limit == size)
        checksum = getPrecalculatedChecksum().value();

    std::optional<UInt128> second_checksum;
    if (second_limit == size)
        second_checksum = getPrecalculatedChecksum().value();

    return {checksum, second_checksum};
}


std::pair<std::optional<UInt128>, std::optional<UInt128>>
BackupEntryWithChecksumCalculation::combinePrecalculatedChecksumWithEncryptionIV(UInt64 limit, std::optional<UInt64> second_limit) const
{
    auto [checksum, second_checksum] = getPrecalculatedChecksumIfFull(limit, second_limit);

    UInt128 iv;
    if (checksum || second_checksum)
        iv = getDisk()->getEncryptedFileIV(getFilePath());

    if (checksum)
        checksum = combineChecksums(*checksum, iv);

    if (second_checksum)
        second_checksum = combineChecksums(*second_checksum, iv);

    return {checksum, second_checksum};
}


std::pair<std::optional<UInt128>, std::optional<UInt128>> BackupEntryWithChecksumCalculation::calculateChecksumFromReading(
    UInt64 limit, std::optional<UInt64> second_limit, const ReadSettings & read_settings) const
{
    UInt64 size = getSize();

    /// Make sure that `limit <= size` and `*second_limit <= size`.
    limit = std::min(limit, size);
    if (second_limit)
        second_limit = std::min(*second_limit, size);

    UInt64 read_size = second_limit.value_or(limit);

    if (getDisk())
    {
        bool is_remote_file = getDisk()->isRemote();
        if (is_remote_file)
        {
            ProfileEvents::increment(ProfileEvents::BackupReadRemoteFilesToCalculateChecksums);
            ProfileEvents::increment(ProfileEvents::BackupReadRemoteBytesToCalculateChecksums, read_size);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::BackupReadLocalFilesToCalculateChecksums);
            ProfileEvents::increment(ProfileEvents::BackupReadLocalBytesToCalculateChecksums, read_size);
        }
    }

    std::unique_ptr<ReadBuffer> read_buffer;
    std::unique_ptr<HashingReadBuffer> hashing_read_buffer;
    UInt64 offset = 0;
    UInt128 current_hash = 0;

    /// Calculates a hash of the file contents at offsets [0..limit_).
    auto calculate_hash = [&](UInt64 limit_) -> std::optional<UInt128>
    {
        if ((limit_ != size) && !isPartialChecksumAllowed())
            return {};
        if (limit_ == offset)
            return current_hash;
        chassert(limit_ > offset);
        if (!read_buffer)
        {
            read_buffer = getReadBuffer(read_settings.adjustBufferSize(read_size));
            hashing_read_buffer = std::make_unique<HashingReadBuffer>(*read_buffer);
        }
        offset += hashing_read_buffer->tryIgnore(limit_ - offset);
        if (offset != limit_)
        {
            /// The file ended before we reach offset `limit_` and we know that `limit_ <= size` (see the beginning of this function),
            /// so now the actual size of the file is less than the value returned by getSize().
            /// That isn't allowed for backups because all files prepared for a backup must be either immutable
            /// or append-only.
            throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE,
                            "Size of file {} decreased ({} -> {}) unexpectedly while calculating its checksum",
                            getFilePath(), size, offset);
        }
        if ((limit_ == size) && !isPartialChecksumAllowed() && !hashing_read_buffer->eof())
        {
            /// We reached offset `size` which is expected to be the end of the file but the file surprisingly didn't end there.
            /// Perhaps it was appended after we got its size for backup but that's not allowed for immutable files
            /// (function `isPartialChecksumAllowed()` returns false for immutable files).
            throw Exception(ErrorCodes::EXPECTED_END_OF_FILE,
                            "Size of immutable file {} increased unexpectedly while calculating its checksum",
                            getFilePath());
        }
        current_hash = hashing_read_buffer->getHash();
        return current_hash;
    };

    std::optional<UInt128> checksum = calculate_hash(limit);

    std::optional<UInt128> second_checksum;
    if (second_limit)
        second_checksum = calculate_hash(*second_limit);

    return {checksum, second_checksum};
}


std::pair<std::optional<UInt128>, std::optional<UInt128>> BackupEntryWithChecksumCalculation::calculateChecksumFromRemotePath(
    UInt64 limit, std::optional<UInt64> second_limit) const
{
    UInt64 size = getSize();

    /// Make sure that `limit <= size` and `*second_limit <= size`.
    limit = std::min(limit, size);
    if (second_limit)
        second_limit = std::min(*second_limit, size);

    /// `stored_objects` represent parts of the file named `getFilePath()` stored on a remote disk.
    /// They are always returned in the same order (`stored_objects[i]` represents i-th part of the file),
    /// so we can use them in that order to calculate a hash.
    std::optional<StoredObjects> stored_objects;

    UInt64 offset = 0;
    UInt128 current_hash = 0;

    /// Calculates a hash of the file contents at offsets [0..limit_).
    auto calculate_hash = [&](UInt64 limit_) -> std::optional<UInt128>
    {
        if ((limit_ != size) && !isPartialChecksumAllowed())
            return {};
        if (offset == limit_)
            return current_hash;
        if (!stored_objects)
            stored_objects = getDisk()->getStorageObjects(getFilePath());
        offset = 0;
        UInt128 hash = 0;
        size_t index = 0;
        while (offset != limit_)
        {
            if (index == stored_objects->size())
            {
                /// The file ended before we reach offset `limit_` and we know that `limit_ <= size` (see the beginning of this function),
                /// so now the actual size of the file is less than the value returned by getSize().
                /// That isn't allowed for backups because all files prepared for a backup must be either immutable
                /// or append-only.
                throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE,
                                "Size of file {} decreased ({} -> {}) unexpectedly while calculating its checksum",
                                getFilePath(), size, getTotalSize(*stored_objects));
            }
            const auto & object = (*stored_objects)[index];
            UInt64 next_offset = std::min(offset + object.bytes_size, limit_);
            UInt64 bytes_count = next_offset - offset;
            if (bytes_count)
            {
                hash = CityHash_v1_0_2::CityHash128WithSeed(object.remote_path.data(), object.remote_path.length(), {hash.items[0], hash.items[1]});
                hash = CityHash_v1_0_2::CityHash128WithSeed(reinterpret_cast<const char *>(&bytes_count), sizeof(bytes_count), {hash.items[0], hash.items[1]});
            }
            offset = next_offset;
            ++index;
        }
        if ((limit_ == size) && !isPartialChecksumAllowed() && (index != stored_objects->size()))
        {
            /// We reached offset `size` which is expected to be the end of the file but the file surprisingly didn't end there.
            /// Perhaps the file was appended after we got its size for backup but that's not allowed for immutable files
            /// (function `isPartialChecksumAllowed()` returns false for immutable files).
            throw Exception(ErrorCodes::EXPECTED_END_OF_FILE,
                            "Size of immutable file {} increased unexpectedly while calculating its checksum",
                            getFilePath());
        }
        current_hash = hash;
        return hash;
    };

    std::optional<UInt128> checksum = calculate_hash(limit);

    std::optional<UInt128> second_checksum;
    if (second_limit)
        second_checksum = calculate_hash(*second_limit);

    return {checksum, second_checksum};
}

}
