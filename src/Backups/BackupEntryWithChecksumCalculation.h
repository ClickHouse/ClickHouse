#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Calculates the checksum and the partial checksum for a backup entry based on ReadBuffer returned by getReadBuffer().
class BackupEntryWithChecksumCalculation : public IBackupEntry
{
public:
    UInt128 getChecksum(const ReadSettings & read_settings) const override;
    std::optional<UInt128> getPartialChecksum(size_t prefix_length, const ReadSettings & read_settings) const override;

protected:
    virtual UInt128 calculateChecksum(const ReadSettings & read_settings) const;
    virtual bool isPartialChecksumAllowed() const { return true; }

private:
    mutable std::optional<UInt128> calculated_checksum;
};

}
