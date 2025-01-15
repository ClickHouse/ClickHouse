#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

UInt128 BackupEntryWithChecksumCalculation::getChecksum(const ReadSettings & read_settings) const
{
    if (calculated_checksum)
        return *calculated_checksum;

    if (getSize() == 0)
        calculated_checksum = 0;
    else
        calculated_checksum = calculateChecksum(read_settings);

    return *calculated_checksum;
}

UInt128 BackupEntryWithChecksumCalculation::calculateChecksum(const ReadSettings & read_settings) const
{
    auto read_buffer = getReadBuffer(read_settings.adjustBufferSize(getSize()));
    HashingReadBuffer hashing_read_buffer(*read_buffer);
    hashing_read_buffer.ignoreAll();
    return hashing_read_buffer.getHash();
}

std::optional<UInt128> BackupEntryWithChecksumCalculation::getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const
{
    if (prefix_length == 0)
        return 0;

    size_t size = getSize();
    if (prefix_length >= size)
        return getChecksum(read_settings);

    if (!isPartialChecksumAllowed())
        return {};

    auto read_buffer = getReadBuffer(read_settings.adjustBufferSize(calculated_checksum ? prefix_length : size));
    HashingReadBuffer hashing_read_buffer(*read_buffer);

    hashing_read_buffer.ignore(prefix_length);
    auto partial_checksum = hashing_read_buffer.getHash();

    if (!calculated_checksum)
    {
        hashing_read_buffer.ignoreAll();
        calculated_checksum = hashing_read_buffer.getHash();
    }

    return partial_checksum;
}

}
