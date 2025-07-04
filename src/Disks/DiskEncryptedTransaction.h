#pragma once

#include "config.h"

#if USE_SSL

#include <Disks/IDiskTransaction.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/JemallocNodumpSTLAllocator.h>

namespace DB
{

namespace FileEncryption { enum class Algorithm : uint8_t; }

struct DiskEncryptedSettings
{

    DiskPtr wrapped_disk;
    String disk_path;
    NoDumpString current_key;
    UInt128 current_key_fingerprint;
    FileEncryption::Algorithm current_algorithm;
    std::unordered_map<UInt128 /* fingerprint */, NoDumpString /* key */> all_keys;

    /// Returns an encryption key found by its fingerprint.
    NoDumpString findKeyByFingerprint(UInt128 key_fingerprint, const String & path_for_logs) const;
};


class DiskEncryptedTransaction : public IDiskTransaction
{
public:
    static String wrappedPath(const String disk_path, const String & path)
    {
        // if path starts_with disk_path -> got already wrapped path
        if (!disk_path.empty() && path.starts_with(disk_path))
            return path;
        return disk_path + path;
    }

    DiskEncryptedTransaction(DiskTransactionPtr delegate_transaction_, const std::string & disk_path_, DiskEncryptedSettings current_settings_, IDisk * delegate_disk_)
        : delegate_transaction(delegate_transaction_)
        , disk_path(disk_path_)
        , current_settings(current_settings_)
        , delegate_disk(delegate_disk_)
    {}

    /// Tries to commit all accumulated operations simultaneously.
    /// If something fails rollback and throw exception.
    void commit() override // NOLINT
    {
        delegate_transaction->commit();
    }

    void undo() override
    {
        delegate_transaction->undo();
    }

    ~DiskEncryptedTransaction() override = default;

    /// Create directory.
    void createDirectory(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->createDirectory(wrapped_path);
    }

    /// Create directory and all parent directories if necessary.
    void createDirectories(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->createDirectories(wrapped_path);
    }

    /// Remove all files from the directory. Directories are not removed.
    void clearDirectory(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->clearDirectory(wrapped_path);
    }

    /// Move directory from `from_path` to `to_path`.
    void moveDirectory(const std::string & from_path, const std::string & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate_transaction->moveDirectory(wrapped_from_path, wrapped_to_path);
    }

    void moveFile(const std::string & from_path, const std::string & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate_transaction->moveFile(wrapped_from_path, wrapped_to_path);

    }

    void createFile(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->createFile(wrapped_path);
    }

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    void replaceFile(const std::string & from_path, const std::string & to_path) override
    {
        auto wrapped_from_path = wrappedPath(from_path);
        auto wrapped_to_path = wrappedPath(to_path);
        delegate_transaction->replaceFile(wrapped_from_path, wrapped_to_path);
    }

    /// Only copy of several files supported now. Disk interface support copy to another disk
    /// but it's impossible to implement correctly in transactions because other disk can
    /// use different metadata storage.
    /// TODO: maybe remove it at all, we don't want copies
    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings) override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const std::string & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {},
        bool autocommit = true) override;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeFile(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeFile(wrapped_path);
    }

    /// Remove file if it exists.
    void removeFileIfExists(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeFileIfExists(wrapped_path);
    }

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    void removeDirectory(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeDirectory(wrapped_path);
    }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    void removeRecursive(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeRecursive(wrapped_path);
    }

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    void removeSharedFile(const std::string & path, bool keep_shared_data) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeSharedFile(wrapped_path, keep_shared_data);
    }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (false) or keep (true) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeSharedRecursive(wrapped_path, keep_all_shared_data, file_names_remove_metadata_only);
    }

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->removeSharedFileIfExists(wrapped_path, keep_shared_data);
    }

    /// Batch request to remove multiple files.
    /// May be much faster for blob storage.
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        for (const auto & file : files)
        {
            auto wrapped_path = wrappedPath(file.path);
            bool keep = keep_all_batch_data || file_names_remove_metadata_only.contains(fs::path(file.path).filename());
            if (file.if_exists)
                delegate_transaction->removeSharedFileIfExists(wrapped_path, keep);
            else
                delegate_transaction->removeSharedFile(wrapped_path, keep);
        }
    }

    /// Set last modified time to file or directory at `path`.
    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->setLastModified(wrapped_path, timestamp);
    }

    /// Just chmod.
    void chmod(const String & path, mode_t mode) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->chmod(wrapped_path, mode);
    }

    /// Set file at `path` as read-only.
    void setReadOnly(const std::string & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->setReadOnly(wrapped_path);
    }

    /// Create hardlink from `src_path` to `dst_path`.
    void createHardLink(const std::string & src_path, const std::string & dst_path) override
    {
        auto wrapped_src_path = wrappedPath(src_path);
        auto wrapped_dst_path = wrappedPath(dst_path);
        delegate_transaction->createHardLink(wrapped_src_path, wrapped_dst_path);
    }

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate_transaction->writeFileUsingBlobWritingFunction(wrapped_path, mode, std::move(write_blob_function));
    }

    std::unique_ptr<WriteBufferFromFileBase> writeEncryptedFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) const
    {
        auto wrapped_path = wrappedPath(path);
        return delegate_transaction->writeFile(wrapped_path, buf_size, mode, settings);
    }

    /// Truncate file to the target size.
    void truncateFile(const std::string & src_path, size_t target_size) override
    {
        auto wrapped_path = wrappedPath(src_path);
        delegate_transaction->truncateFile(wrapped_path, target_size);
    }


private:

    String wrappedPath(const String & path) const
    {
        return wrappedPath(disk_path, path);
    }

    DiskTransactionPtr delegate_transaction;
    std::string disk_path;
    DiskEncryptedSettings current_settings;
    IDisk * delegate_disk;
};

}

#endif
