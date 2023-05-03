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

    if (prefix_length >= this->getSize())
        return this->getChecksum();

    auto read_buffer = this->getReadBuffer(ReadSettings{}.adjustBufferSize(prefix_length));
    HashingReadBuffer hashing_read_buffer(*read_buffer);
    hashing_read_buffer.ignore(prefix_length);
    auto partial_checksum = hashing_read_buffer.getHash();

    std::lock_guard lock{checksum_calculation_mutex};
    if (!calculated_checksum)
    {
        hashing_read_buffer.ignoreAll();
        calculated_checksum = hashing_read_buffer.getHash();
    }

    return partial_checksum;
}

template class BackupEntryWithChecksumCalculation<IBackupEntry>;

}
