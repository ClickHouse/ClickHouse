#pragma once

#include <Backups/BackupImpl.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory.
class BackupInDirectory : public BackupImpl
{
public:
    BackupInDirectory(
        const String & backup_name_,
        OpenMode open_mode_,
        const DiskPtr & disk_,
        const String & path_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});
    ~BackupInDirectory() override;

private:
    bool backupExists() const override;
    void startWriting() override;
    void removeAllFilesAfterFailure() override;
    ReadBufferCreator readFileImpl(const String & file_name) const override;
    void addFileImpl(const String & file_name, ReadBuffer & read_buffer) override;

    DiskPtr disk;
    String path;
    String dir_path; /// `path` without terminating slash
};

}
