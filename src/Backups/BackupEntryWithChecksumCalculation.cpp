#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

template <typename Base>
UInt128 BackupEntryWithChecksumCalculation<Base>::getChecksum() const
{
    std::lock_guard lock{checksum_calculation_mutex};
    if (!calculated_checksum)
    {
        auto read_buffer = this->getReadBuffer(ReadSettings{}.adjustBufferSize(this->getSize()));
        HashingReadBuffer hashing_read_buffer(*read_buffer);
        hashing_read_buffer.ignoreAll();
        calculated_checksum = hashing_read_buffer.getHash();
    }
    return *calculated_checksum;
}

template <typename Base>
std::optional<UInt128> BackupEntryWithChecksumCalculation<Base>::getPartialChecksum(size_t prefix_length) const
{
    if (prefix_length == 0)
        return 0;

    size_t size = this->getSize();
    if (prefix_length >= size)
        return this->getChecksum();

    std::lock_guard lock{checksum_calculation_mutex};

    ReadSettings read_settings;
    if (calculated_checksum)
        read_settings.adjustBufferSize(calculated_checksum ? prefix_length : size);

    auto read_buffer = this->getReadBuffer(read_settings);
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

template class BackupEntryWithChecksumCalculation<IBackupEntry>;

}
