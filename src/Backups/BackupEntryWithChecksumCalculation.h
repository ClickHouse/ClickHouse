#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Calculates the checksum and the partial checksum for a backup entry based on ReadBuffer returned by getReadBuffer().
class BackupEntryWithChecksumCalculation : public IBackupEntry
{
public:
    UInt128 getChecksum(const ReadSettings & read_settings) const override;
    std::optional<UInt128> getPartialChecksum(UInt64 limit, const ReadSettings & read_settings) const override;

protected:
    virtual bool isPartialChecksumAllowed() const { return true; }
    virtual UInt128 calculateChecksum(UInt64 limit, std::optional<UInt64> second_limit, UInt128 * second_checksum, const ReadSettings & read_settings) const;

    UInt128 calculateChecksumFromReadBuffer(UInt64 limit,
                                            std::optional<UInt64> second_limit, UInt128 * second_checksum,
                                            const ReadSettings & read_settings) const;

private:
    mutable std::optional<UInt128> calculated_checksum;
};

}
