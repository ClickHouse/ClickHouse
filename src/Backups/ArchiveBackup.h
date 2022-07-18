#pragma once

#include <Backups/BackupImpl.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class IArchiveReader;
class IArchiveWriter;

/// Stores a backup as a single .zip file.
class ArchiveBackup : public BackupImpl
{
public:
    /// `disk`_ is allowed to be nullptr and that means the `path_` is a path in the local filesystem.
    ArchiveBackup(
        const String & backup_name_,
        const DiskPtr & disk_,
        const String & path_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});

    ~ArchiveBackup() override;

    static constexpr const int kDefaultCompressionLevel = -1;

    /// Sets compression method and level.
    void setCompression(const String & compression_method_, int compression_level_ = kDefaultCompressionLevel);

    /// Sets password.
    void setPassword(const String & password_);

private:
    bool backupExists() const override;
    void openImpl(OpenMode open_mode_) override;
    void closeImpl(const Strings & written_files_, bool writing_finalized_) override;
    bool supportsWritingInMultipleThreads() const override { return false; }
    std::unique_ptr<ReadBuffer> readFileImpl(const String & file_name) const override;
    std::unique_ptr<WriteBuffer> writeFileImpl(const String & file_name) override;

    const DiskPtr disk;
    const String path;
    std::shared_ptr<IArchiveReader> reader;
    std::shared_ptr<IArchiveWriter> writer;
    String compression_method;
    int compression_level = kDefaultCompressionLevel;
    String password;
};

}
