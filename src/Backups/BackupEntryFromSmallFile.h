#pragma once

#include <Backups/BackupEntryFromMemory.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup,
/// assuming that the file is small and can be easily loaded into memory.
class BackupEntryFromSmallFile : public BackupEntryFromMemory
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

    String getFilePath() const { return file_path; }
    DiskPtr getDisk() const { return disk; }

private:
    const DiskPtr disk;
    const String file_path;
};

}
