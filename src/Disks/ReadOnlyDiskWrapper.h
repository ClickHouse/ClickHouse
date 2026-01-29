#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ReadOnlyDiskWrapper : public IDisk
{
public:
    explicit ReadOnlyDiskWrapper(const DiskPtr & delegate_) : IDisk(fmt::format("{}-readonly", delegate_->getName())), delegate(delegate_) {}
    ~ReadOnlyDiskWrapper() override = default;

    DiskTransactionPtr createTransaction() override { return delegate->createTransaction(); }
    const String & getName() const override { return delegate->getName(); }
    const String & getPath() const override { return delegate->getPath(); }
    std::optional<UInt64> getTotalSpace() const override { return delegate->getTotalSpace(); }
    std::optional<UInt64> getAvailableSpace() const override { return delegate->getAvailableSpace(); }
    std::optional<UInt64> getUnreservedSpace() const override { return delegate->getUnreservedSpace(); }
    UInt64 getKeepingFreeSpace() const override { return delegate->getKeepingFreeSpace(); }
    bool existsFile(const String & path) const override { return delegate->existsFile(path); }
    bool existsDirectory(const String & path) const override { return delegate->existsDirectory(path); }
    bool existsFileOrDirectory(const String & path) const override { return delegate->existsFileOrDirectory(path); }
    size_t getFileSize(const String & path) const override { return delegate->getFileSize(path); }

    Strings getBlobPath(const String & path) const override { return delegate->getBlobPath(path); }
    bool areBlobPathsRandom() const override { return delegate->areBlobPathsRandom(); }
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        delegate->writeFileUsingBlobWritingFunction(path, mode, std::move(write_blob_function));
    }

    DirectoryIteratorPtr iterateDirectory(const String & path) const override { return delegate->iterateDirectory(path); }

    void copyDirectoryContent(
        const String & from_dir,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_dir,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override
    {
        delegate->copyDirectoryContent(from_dir, to_disk, to_dir, read_settings, write_settings, cancellation_hook);
    }

    void listFiles(const String & path, std::vector<String> & file_names) const override { delegate->listFiles(path, file_names); }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const override { return delegate->readFile(path, settings, read_hint); }

    time_t getLastChanged(const String & path) const override { return delegate->getLastChanged(path); }
    Poco::Timestamp getLastModified(const String & path) const override { return delegate->getLastModified(path); }
    void setReadOnly(const String & path) override { delegate->setReadOnly(path); }
    void truncateFile(const String & path, size_t size) override { delegate->truncateFile(path, size); }
    String getUniqueId(const String & path) const override { return delegate->getUniqueId(path); }
    bool checkUniqueId(const String & id) const override { return delegate->checkUniqueId(id); }
    DataSourceDescription getDataSourceDescription() const override { return delegate->getDataSourceDescription(); }
    bool isRemote() const override { return delegate->isRemote(); }

    bool isWriteOnce() const override { return delegate->isWriteOnce(); }
    bool supportZeroCopyReplication() const override { return delegate->supportZeroCopyReplication(); }
    bool supportParallelWrite() const override { return delegate->supportParallelWrite(); }
    SyncGuardPtr getDirectorySyncGuard(const String & path) const override { return delegate->getDirectorySyncGuard(path); }
    void shutdown() override { delegate->shutdown(); }
    void startupImpl() override { delegate->startupImpl(); }
    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & map) override
    {
        delegate->applyNewSettings(config, context, config_prefix, map);
    }

    bool supportsCache() const override { return delegate->supportsCache(); }

    StoredObjects getStorageObjects(const String & path) const override { return delegate->getStorageObjects(path); }

    DiskObjectStoragePtr createDiskObjectStorage() override { return delegate->createDiskObjectStorage(); }
    ObjectStoragePtr getObjectStorage() override { return delegate->getObjectStorage(); }
    NameSet getCacheLayersNames() const override { return delegate->getCacheLayersNames(); }

    MetadataStoragePtr getMetadataStorage() override { return delegate->getMetadataStorage(); }

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override { return delegate->getSerializedMetadata(file_paths); }

    UInt32 getRefCount(const String & path) const override { return delegate->getRefCount(path); }

    void syncRevision(UInt64 revision) override { delegate->syncRevision(revision); }

    UInt64 getRevision() const override { return delegate->getRevision(); }

    bool supportsStat() const override { return delegate->supportsStat(); }
    struct stat stat(const String & path) const override { return delegate->stat(path); }

    bool supportsChmod() const override { return delegate->supportsChmod(); }
    void chmod(const String & path, mode_t mode) override { delegate->chmod(path, mode); }

    bool isReadOnly() const override { return true; }
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String &, size_t, WriteMode, const WriteSettings &) override { throwNotAllowed(); }
    void moveFile(const String &, const String &) override { throwNotAllowed(); }
    void replaceFile(const String &, const String &) override { throwNotAllowed(); }
    void removeFile(const String &) override { throwNotAllowed(); }
    void removeFileIfExists(const String &) override { throwNotAllowed(); }
    ReservationPtr reserve(UInt64 /*bytes*/) override { throwNotAllowed(); }
    void removeRecursive(const String &) override { throwNotAllowed(); }
    void removeSharedFile(const String &, bool) override { throwNotAllowed(); }
    void removeSharedFileIfExists(const String &, bool) override { throwNotAllowed(); }
    void removeSharedRecursive(const String &, bool, const NameSet &) override { throwNotAllowed(); }
    void moveDirectory(const String &, const String &) override { throwNotAllowed(); }
    void removeDirectory(const String &) override { throwNotAllowed(); }
    void setLastModified(const String &, const Poco::Timestamp &) override { throwNotAllowed(); }
    void createFile(const String &) override { throwNotAllowed(); }
    void createDirectory(const String &) override { throwNotAllowed(); }
    void createDirectories(const String &) override { throwNotAllowed(); }
    void createHardLink(const String &, const String &) override { throwNotAllowed(); }

private:

    [[noreturn]] void throwNotAllowed() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation not allowed, disk {} is read-only", getName());
    }

    DiskPtr delegate;
};

}
