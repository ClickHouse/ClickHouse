#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

template <typename Base>
UInt128 BackupEntryWithChecksumCalculation<Base>::getChecksum(const ReadSettings & read_settings) const
{
    {
        std::lock_guard lock{checksum_calculation_mutex};
        if (calculated_checksum)
            return *calculated_checksum;
    }

    size_t size = this->getSize();

    {
        std::lock_guard lock{checksum_calculation_mutex};
        if (!calculated_checksum)
        {
            if (size == 0)
            {
                calculated_checksum = 0;
            }
            else
            {
                auto read_buffer = this->getReadBuffer(read_settings.adjustBufferSize(size));
                HashingReadBuffer hashing_read_buffer(*read_buffer);
                hashing_read_buffer.ignoreAll();
                calculated_checksum = hashing_read_buffer.getHash();
            }
        }
        return *calculated_checksum;
    }
}

template <typename Base>
std::optional<UInt128> BackupEntryWithChecksumCalculation<Base>::getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const
{
    if (prefix_length == 0)
        return 0;

    size_t size = this->getSize();
    if (prefix_length >= size)
        return this->getChecksum(read_settings);

    std::lock_guard lock{checksum_calculation_mutex};

    auto read_buffer = this->getReadBuffer(read_settings.adjustBufferSize(calculated_checksum ? prefix_length : size));
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
