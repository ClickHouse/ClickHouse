#pragma once

#include <Backups/BackupImpl.h>
#include <filesystem>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory.
class DirectoryBackup : public BackupImpl
{
public:
    /// `disk`_ is allowed to be nullptr and that means the `path_` is a path in the local filesystem.
    DirectoryBackup(
        const String & backup_name_,
        const DiskPtr & disk_,
        const String & path_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});
    ~DirectoryBackup() override;

private:
    bool backupExists() const override;
    void openImpl(OpenMode open_mode_) override;
    void closeImpl(const Strings & written_files_, bool writing_finalized_) override;
    std::unique_ptr<ReadBuffer> readFileImpl(const String & file_name) const override;
    std::unique_ptr<WriteBuffer> writeFileImpl(const String & file_name) override;

    DiskPtr disk;
    std::filesystem::path path;
};

}
