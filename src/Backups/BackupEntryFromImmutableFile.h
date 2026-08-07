#pragma once

#include <Backups/BackupEntryWithChecksumCalculation.h>
#include <base/defines.h>
#include <mutex>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup, assuming that until this backup entry is destroyed the file won't be changed.
class BackupEntryFromImmutableFile : public BackupEntryWithChecksumCalculation
{
public:
    /// The constructor is allowed to not set `file_size_` or `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromImmutableFile(
        const DiskPtr & disk_,
        const String & file_path_,
        bool copy_encrypted_ = false,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {},
        bool allow_checksum_from_remote_path_ = true);

    ~BackupEntryFromImmutableFile() override;

    std::unique_ptr<SeekableReadBuffer> getReadBuffer(const ReadSettings & read_settings) const override;
    UInt64 getSize() const override;

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }
    bool isEncryptedByDisk() const override { return copy_encrypted; }
    bool isFromFile() const override { return true; }
    bool isFromImmutableFile() const override { return true; }
    DiskPtr getDisk() const override { return disk; }
    String getFilePath() const override { return file_path; }

protected:
    std::optional<UInt128> getPrecalculatedChecksum() const override { return passed_checksum; }
    bool isPartialChecksumAllowed() const override { return false; }
    bool isChecksumFromRemotePathAllowed() const override { return allow_checksum_from_remote_path; }

private:
    UInt64 calculateSize() const;

    const DiskPtr disk;
    const String file_path;
    const DataSourceDescription data_source_description;
    const bool copy_encrypted;
    const std::optional<UInt64> passed_file_size;
    const std::optional<UInt128> passed_checksum;
    const bool allow_checksum_from_remote_path;
    mutable std::optional<UInt64> calculated_size TSA_GUARDED_BY(mutex);
};

}
