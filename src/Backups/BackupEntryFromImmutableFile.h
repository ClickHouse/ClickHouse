#pragma once

#include <Backups/IBackupEntry.h>
#include <base/defines.h>
#include <mutex>

namespace Poco { class TemporaryFile; }

namespace DB
{
class TemporaryFileOnDisk;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup, assuming that until this backup entry is destroyed the file won't be changed.
class BackupEntryFromImmutableFile : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size_` or `checksum_`, in that case it will be calculated from the data.
    explicit BackupEntryFromImmutableFile(
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {},
        const std::shared_ptr<Poco::TemporaryFile> & temporary_file_ = {});

    BackupEntryFromImmutableFile(
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {},
        const std::shared_ptr<TemporaryFileOnDisk> & temporary_file_ = {});

    ~BackupEntryFromImmutableFile() override;

    UInt64 getSize() const override;
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override;

    String getFilePath() const { return file_path; }
    DiskPtr getDisk() const { return disk; }

private:
    const DiskPtr disk;
    const String file_path;
    mutable std::optional<UInt64> file_size TSA_GUARDED_BY(get_file_size_mutex);
    mutable std::mutex get_file_size_mutex;
    const std::optional<UInt128> checksum;
    const std::shared_ptr<Poco::TemporaryFile> temporary_file;
    const std::shared_ptr<TemporaryFileOnDisk> temporary_file_on_disk;
};

}
