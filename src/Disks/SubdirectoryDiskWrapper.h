#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>

namespace DB
{

/// A view of a subdirectory of another disk: every path passed to this disk is resolved inside
/// `sub_path` of the delegate, and `getPath` returns the delegate path extended by `sub_path`.
/// It is used for `user_files_policy`, where `user_files_path` names a directory inside the disk.
///
/// Operations that would expose the namespace of the delegate (its metadata storage, object storage,
/// reservations, the delegate itself) are not forwarded and throw, so that nothing can address a file
/// outside `sub_path` through this disk. Transactions and copying use the default implementations of
/// `IDisk`, which call the methods of this disk and therefore stay inside `sub_path` too.
class SubdirectoryDiskWrapper : public IDisk
{
public:
    /// `sub_path_` must be a normalized path relative to the root of `delegate_`, without leading or trailing slashes.
    SubdirectoryDiskWrapper(const DiskPtr & delegate_, const String & sub_path_);
    ~SubdirectoryDiskWrapper() override = default;

    const String & getName() const override { return delegate->getName(); }
    const String & getPath() const override { return path; }

    std::optional<UInt64> getTotalSpace() const override { return delegate->getTotalSpace(); }
    std::optional<UInt64> getAvailableSpace() const override { return delegate->getAvailableSpace(); }
    std::optional<UInt64> getUnreservedSpace() const override { return delegate->getUnreservedSpace(); }
    UInt64 getKeepingFreeSpace() const override { return delegate->getKeepingFreeSpace(); }

    ReservationPtr reserve(UInt64 bytes) override;
    ReservationPtr reserve(UInt64 bytes, const ReservationConstraints & constraints) override;

    bool existsFile(const String & file_path) const override { return delegate->existsFile(wrap(file_path)); }
    bool existsDirectory(const String & dir_path) const override { return delegate->existsDirectory(wrap(dir_path)); }
    bool existsFileOrDirectory(const String & file_path) const override { return delegate->existsFileOrDirectory(wrap(file_path)); }
    size_t getFileSize(const String & file_path) const override { return delegate->getFileSize(wrap(file_path)); }

    void createDirectory(const String & dir_path) override { delegate->createDirectory(wrap(dir_path)); }
    void createDirectories(const String & dir_path) override { delegate->createDirectories(wrap(dir_path)); }
    void moveDirectory(const String & from_path, const String & to_path) override { delegate->moveDirectory(wrap(from_path), wrap(to_path)); }
    DirectoryIteratorPtr iterateDirectory(const String & dir_path) const override;
    bool isDirectoryEmpty(const String & dir_path) const override { return delegate->isDirectoryEmpty(wrap(dir_path)); }

    void createFile(const String & file_path) override { delegate->createFile(wrap(file_path)); }
    void moveFile(const String & from_path, const String & to_path) override { delegate->moveFile(wrap(from_path), wrap(to_path)); }
    void replaceFile(const String & from_path, const String & to_path) override { delegate->replaceFile(wrap(from_path), wrap(to_path)); }

    void listFiles(const String & dir_path, std::vector<String> & file_names) const override { delegate->listFiles(wrap(dir_path), file_names); }

    void prepareRead(
        const String & file_path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const override
    {
        delegate->prepareRead(wrap(file_path), settings, read_hint, pipeline);
    }

    std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const String & file_path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & file_path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void removeFile(const String & file_path) override { delegate->removeFile(wrap(file_path)); }
    void removeFileIfExists(const String & file_path) override { delegate->removeFileIfExists(wrap(file_path)); }
    void removeDirectory(const String & dir_path) override { delegate->removeDirectory(wrap(dir_path)); }
    void removeDirectoryIfExists(const String & dir_path) override { delegate->removeDirectoryIfExists(wrap(dir_path)); }
    void removeRecursive(const String & dir_path) override { delegate->removeRecursive(wrap(dir_path)); }
    void removeRecursiveWithLimit(const String & dir_path) override { delegate->removeRecursiveWithLimit(wrap(dir_path)); }
    void removeSharedFile(const String & file_path, bool keep_shared_data) override { delegate->removeSharedFile(wrap(file_path), keep_shared_data); }
    void removeSharedFileIfExists(const String & file_path, bool keep_shared_data) override
    {
        delegate->removeSharedFileIfExists(wrap(file_path), keep_shared_data);
    }
    void removeSharedRecursive(const String & dir_path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override
    {
        delegate->removeSharedRecursive(wrap(dir_path), keep_all_shared_data, file_names_remove_metadata_only);
    }

    Strings getBlobPath(const String & file_path) const override { return delegate->getBlobPath(wrap(file_path)); }
    bool areBlobPathsRandom() const override { return delegate->areBlobPathsRandom(); }
    void writeFileUsingBlobWritingFunction(const String & file_path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        delegate->writeFileUsingBlobWritingFunction(wrap(file_path), mode, std::move(write_blob_function));
    }

    void setLastModified(const String & file_path, const Poco::Timestamp & timestamp) override { delegate->setLastModified(wrap(file_path), timestamp); }
    Poco::Timestamp getLastModified(const String & file_path) const override { return delegate->getLastModified(wrap(file_path)); }
    time_t getLastChanged(const String & file_path) const override { return delegate->getLastChanged(wrap(file_path)); }
    void setReadOnly(const String & file_path) override { delegate->setReadOnly(wrap(file_path)); }
    void createHardLink(const String & src_path, const String & dst_path) override { delegate->createHardLink(wrap(src_path), wrap(dst_path)); }
    void truncateFile(const String & file_path, size_t size) override { delegate->truncateFile(wrap(file_path), size); }

    bool supportsStat() const override { return delegate->supportsStat(); }
    struct stat stat(const String & file_path) const override { return delegate->stat(wrap(file_path)); }
    bool supportsChmod() const override { return delegate->supportsChmod(); }
    void chmod(const String & file_path, mode_t mode) override { delegate->chmod(wrap(file_path), mode); }

    SyncGuardPtr getDirectorySyncGuard(const String & dir_path) const override { return delegate->getDirectorySyncGuard(wrap(dir_path)); }

    DataSourceDescription getDataSourceDescription() const override { return delegate->getDataSourceDescription(); }
    bool isRemote() const override { return delegate->isRemote(); }
    bool supportZeroCopyReplication() const override { return delegate->supportZeroCopyReplication(); }
    bool supportParallelWrite() const override { return delegate->supportParallelWrite(); }
    bool isReadOnly() const override { return delegate->isReadOnly(); }
    bool isPlain() const override { return delegate->isPlain(); }
    bool isWriteOnce() const override { return delegate->isWriteOnce(); }
    bool supportsHardLinks() const override { return delegate->supportsHardLinks(); }
    bool prefersRecursiveRemoval() const override { return delegate->prefersRecursiveRemoval(); }
    bool isBroken() const override { return delegate->isBroken(); }
    bool supportsCache() const override { return delegate->supportsCache(); }
    void refresh(UInt64 not_sooner_than_milliseconds) override { delegate->refresh(not_sooner_than_milliseconds); }

    /// The delegate is owned by the disk selector, which starts it up and shuts it down.
    void startupImpl() override {}
    void shutdown() override {}

private:
    String wrap(const String & file_path) const;

    DiskPtr delegate;
    String sub_path;
    String path;
};

}
