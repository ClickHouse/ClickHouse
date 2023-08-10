#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Calculates the checksum and the partial checksum for a backup entry based on ReadBuffer returned by getReadBuffer().
template <typename Base>
class BackupEntryWithChecksumCalculation : public Base
{
public:
    UInt128 getChecksum(const ReadSettings & read_settings) const override;
    std::optional<UInt128> getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const override;

private:
    mutable std::optional<UInt128> calculated_checksum;
    mutable std::mutex checksum_calculation_mutex;
};

}
