#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup,
/// assuming that the file is small and can be easily loaded into memory.
class BackupEntryFromSmallFile : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    explicit BackupEntryFromSmallFile(
        const String & file_path_,
        const std::optional<UInt128> & checksum_ = {});

    BackupEntryFromSmallFile(
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt128> & checksum_ = {});

    UInt64 getSize() const override { return data.size(); }
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override;

    bool isEncryptedByDisk() const override { return data_source_description.is_encrypted; }

    bool isFromFile() const override { return true; }
    DiskPtr getDisk() const override { return disk; }
    String getFilePath() const override { return file_path; }

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }

private:
    const DiskPtr disk;
    const String file_path;
    const DataSourceDescription data_source_description;
    const String data;
    const std::optional<UInt128> checksum;
};

}
