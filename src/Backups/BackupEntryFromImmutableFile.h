#pragma once

#include <Backups/IBackupEntry.h>
#include <IO/ReadSettings.h>
#include <base/defines.h>
#include <mutex>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup, assuming that until this backup entry is destroyed the file won't be changed.
class BackupEntryFromImmutableFile : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size_` or `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromImmutableFile(
        const DiskPtr & disk_,
        const String & file_path_,
        const ReadSettings & settings_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});

    ~BackupEntryFromImmutableFile() override;

    UInt64 getSize() const override;
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override;
    
    bool isEncryptedByDisk() const override { return data_source_description.is_encrypted; }

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }

    bool isFromFile() const override { return true; }
    bool isFromImmutableFile() const override { return true; }
    DiskPtr getDisk() const override { return disk; }
    String getFilePath() const override { return file_path; }

private:
    const DiskPtr disk;
    const String file_path;
    const DataSourceDescription data_source_description;
    ReadSettings settings;
    mutable std::optional<UInt64> file_size TSA_GUARDED_BY(get_file_size_mutex);
    mutable std::mutex get_file_size_mutex;
    const std::optional<UInt128> checksum;
};

}
