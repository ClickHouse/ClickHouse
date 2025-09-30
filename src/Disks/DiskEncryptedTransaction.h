#pragma once

#include <memory>
#include <IO/ReadBufferFromFileBase.h>
#include <config.h>

#if USE_SSL

#include <Disks/IDiskTransaction.h>
#include <Disks/IDisk.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace FileEncryption { enum class Algorithm : uint8_t; }

struct DiskEncryptedSettings
{
    DiskPtr wrapped_disk;
    String disk_path;
    String current_key;
    UInt128 current_key_fingerprint;
    FileEncryption::Algorithm current_algorithm;
    std::unordered_map<UInt128 /* fingerprint */, String /* key */> all_keys;

    /// Returns an encryption key found by its fingerprint.
    String findKeyByFingerprint(UInt128 key_fingerprint, const String & path_for_logs) const;
};


class DiskEncryptedTransaction : public IDiskTransaction, public std::enable_shared_from_this<DiskEncryptedTransaction>
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
    {
        LOG_DEBUG(getLogger("DiskEncryptedTransaction"),
            "Creating DiskEncryptedTransaction for delegating disk {} at path {}",
            delegate_disk->getName(), disk_path);
    }

    /// Tries to commit all accumulated operations simultaneously.
    /// If something fails rollback and throw exception.
    void commit(const TransactionCommitOptionsVariant & options) override
    {
        delegate_transaction->commit(options);
        LOG_DEBUG(getLogger("DiskEncryptedTransaction"),
            "Commit DiskEncryptedTransaction for delegating disk {} at path {}",
            delegate_disk->getName(), disk_path);
    }
    void commit() override { commit(NoCommitOptions{}); }

    void undo() noexcept override
    {
        delegate_transaction->undo();
    }

    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override
    {
        return delegate_transaction->tryCommit(options);
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
    std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

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
        auto wrapped_path_files = files;
        for (auto & file : wrapped_path_files)
            file.path = wrappedPath(file.path);

        delegate_transaction->removeSharedFiles(wrapped_path_files, keep_all_batch_data, file_names_remove_metadata_only);
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
        return delegate_transaction->writeFileWithAutoCommit(wrapped_path, buf_size, mode, settings);
    }

    /// Truncate file to the target size.
    void truncateFile(const std::string & src_path, size_t size) override
    {
        auto wrapped_path = wrappedPath(src_path);
        delegate_transaction->truncateFile(wrapped_path, size);
    }

    std::vector<std::string> listUncommittedDirectoryInTransaction(const std::string & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate_transaction->listUncommittedDirectoryInTransaction(wrapped_path);
    }

    std::unique_ptr<ReadBufferFromFileBase> readUncommittedFileInTransaction(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint) const override;

    bool isTransactional() const override
    {
        return delegate_transaction->isTransactional();
    }

    void validateTransaction(std::function<void (IDiskTransaction&)> check_function) override
    {
        auto wrapped = [tx = shared_from_this(), moved_func = std::move(check_function)] (IDiskTransaction&)
        {
            moved_func(*tx);
        };
        delegate_transaction->validateTransaction(std::move(wrapped));
    }

private:
    std::unique_ptr<WriteBufferFromFileBase> writeFileImpl(
        bool autocommit,
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings);

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
