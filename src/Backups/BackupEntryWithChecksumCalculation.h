#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Calculates the checksum and a partial checksum for a backup entry.
class BackupEntryWithChecksumCalculation : public IBackupEntry
{
public:
    UInt128 getChecksum(const ReadSettings & read_settings) const override;
    std::optional<UInt128> getPartialChecksum(UInt64 limit, const ReadSettings & read_settings) const override;

protected:
    /// Returns a precalculated checksum for this file if any.
    virtual std::optional<UInt128> getPrecalculatedChecksum() const { return {}; }

    /// Whether it is allowed to calculate a checksum of a part of the file from its beginning up to some point.
    virtual bool isPartialChecksumAllowed() const { return true; }

    mutable std::mutex mutex;

private:
    std::optional<UInt128> calculateChecksum(UInt64 limit, const ReadSettings & read_settings) const;

    /// Calculates one or two checksums.
    std::pair<std::optional<UInt128>, std::optional<UInt128>> calculateChecksum(
        UInt64 limit, std::optional<UInt64> second_limit, const ReadSettings & read_settings) const;

    /// Depending on how a file is stored different methods of calculating its checksum for a backup can be used.
    enum class ChecksumCalculationMethod
    {
        /// Empty file, checksum is 0.
        EmptyZero,

        /// The checksum was calculated and stored somewhere before we started making a backup,
        /// for example all files listed in part's "checksums.txt".
        Precalculated,

        /// Similar to "Precalculated" for the case when a file is stored on an encrypted disk.
        /// It means the precalculated checksum is going to be combined with the initialization vector for that file.
        PrecalculatedCombinedWithEncryptionIV,

        /// Reads a file and calculates a checksum from its contents.
        /// That can be slow for big files and remote disks.
        FromReading,
    };

    /// Chooses an appropriate checksum calculation method based on the disk settings and the file size.
    ChecksumCalculationMethod chooseChecksumCalculationMethod() const;

    bool hasPrecalculatedChecksum() const;

    /// Calculates one or two checksums for method Precalculated.
    std::pair<std::optional<UInt128>, std::optional<UInt128>> getPrecalculatedChecksumIfFull(
        UInt64 limit, std::optional<UInt64> second_limit) const;

    /// Calculates one or two checksums for method PrecalculatedCombinedWithEncryptionIV.
    std::pair<std::optional<UInt128>, std::optional<UInt128>> combinePrecalculatedChecksumWithEncryptionIV(
        UInt64 limit, std::optional<UInt64> second_limit) const;

    /// Calculates one or two checksums for method FromReading.
    std::pair<std::optional<UInt128>, std::optional<UInt128>> calculateChecksumFromReading(
        UInt64 limit, std::optional<UInt64> second_limit, const ReadSettings & read_settings) const;

    mutable std::optional<UInt128> calculated_checksum TSA_GUARDED_BY(mutex);
};

}
