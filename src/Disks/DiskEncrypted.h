#pragma once

#include "config.h"

#if USE_SSL
#include <Disks/IDisk.h>
#include <Common/MultiVersion.h>
#include <Disks/FakeDiskTransaction.h>
#include <Disks/DiskEncryptedTransaction.h>


namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

/// Encrypted disk ciphers all written files on the fly and writes the encrypted files to an underlying (normal) disk.
/// And when we read files from an encrypted disk it deciphers them automatically,
/// so we can work with a encrypted disk like it's a normal disk.
class DiskEncrypted : public IDisk
{
public:
    DiskEncrypted(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_);
    DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_,
                  const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_);
    DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_);

    const String & getName() const override { return encrypted_name; }
    const String & getPath() const override { return disk_absolute_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    bool existsFile(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->existsFile(wrapped_path);
    }

    bool existsDirectory(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->existsDirectory(wrapped_path);
    }

    bool existsFileOrDirectory(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->existsFileOrDirectory(wrapped_path);
    }

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->createDirectory(path);
        tx->commit();
    }

    void createDirectories(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        /// Delegate disk can have retry logic for recursive directory creation. Let it handle it.
        delegate->createDirectories(wrapped_path);
    }

    void clearDirectory(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->clearDirectory(path);
        tx->commit();
    }

    void moveDirectory(const String & from_path, const String & to_path) override
    {
        auto tx = createEncryptedTransaction();
        tx->moveDirectory(from_path, to_path);
        tx->commit();
    }

    DirectoryIteratorPtr iterateDirectory(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->iterateDirectory(wrapped_path);
    }

    void createFile(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->createFile(path);
        tx->commit();
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        auto tx = createEncryptedTransaction();
        tx->moveFile(from_path, to_path);
        tx->commit();
    }

    void replaceFile(const String & from_path, const String & to_path) override
    {
        auto tx = createEncryptedTransaction();
        tx->replaceFile(from_path, to_path);
        tx->commit();
    }

    void listFiles(const String & path, std::vector<String> & file_names) const override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->listFiles(wrapped_path, file_names);
    }

    void copyDirectoryContent(
        const String & from_dir,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_dir,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override
    {
        auto tx = createEncryptedTransaction();
        auto result = tx->writeFile(path, buf_size, mode, settings);
        return result;
    }

    void removeFile(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeFile(path);
        tx->commit();
    }

    void removeFileIfExists(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeFileIfExists(path);
        tx->commit();
    }

    void removeDirectory(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeDirectory(path);
        tx->commit();
    }

    void removeRecursive(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeRecursive(path);
        tx->commit();
    }

    void removeSharedFile(const String & path, bool flag) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeSharedFile(path, flag);
        tx->commit();
    }

    void removeSharedRecursive(const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeSharedRecursive(path, keep_all_batch_data, file_names_remove_metadata_only);
        tx->commit();
    }

    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeSharedFiles(files, keep_all_batch_data, file_names_remove_metadata_only);
        tx->commit();
    }

    void removeSharedFileIfExists(const String & path, bool flag) override
    {
        auto tx = createEncryptedTransaction();
        tx->removeSharedFileIfExists(path, flag);
        tx->commit();
    }

    Strings getBlobPath(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getBlobPath(wrapped_path);
    }

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        auto tx = createEncryptedTransaction();
        tx->writeFileUsingBlobWritingFunction(path, mode, std::move(write_blob_function));
        tx->commit();
    }

    std::unique_ptr<ReadBufferFromFileBase> readEncryptedFile(const String & path, const ReadSettings & settings) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->readFile(wrapped_path, settings);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeEncryptedFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) const override
    {
        auto tx = createEncryptedTransaction();
        auto buf = tx->writeEncryptedFile(path, buf_size, mode, settings);
        return buf;
    }

    size_t getEncryptedFileSize(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getFileSize(wrapped_path);
    }

    size_t getEncryptedFileSize(size_t unencrypted_size) const override;

    UInt128 getEncryptedFileIV(const String & path) const override;

    static size_t convertFileSizeToEncryptedFileSize(size_t file_size);

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override
    {
        auto tx = createEncryptedTransaction();
        tx->setLastModified(path, timestamp);
        tx->commit();
    }

    Poco::Timestamp getLastModified(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getLastModified(wrapped_path);
    }

    time_t getLastChanged(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getLastChanged(wrapped_path);
    }

    void setReadOnly(const String & path) override
    {
        auto tx = createEncryptedTransaction();
        tx->setReadOnly(path);
        tx->commit();
    }

    void createHardLink(const String & src_path, const String & dst_path) override
    {
        auto tx = createEncryptedTransaction();
        tx->createHardLink(src_path, dst_path);
        tx->commit();
    }

    void truncateFile(const String & path, size_t size) override;

    String getUniqueId(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getUniqueId(wrapped_path);
    }

    bool checkUniqueId(const String & id) const override
    {
        return delegate->checkUniqueId(id);
    }

    void onFreeze(const String & path) override
    {
        auto wrapped_path = wrappedPath(path);
        delegate->onFreeze(wrapped_path);
    }

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & map) override;

    DataSourceDescription getDataSourceDescription() const override
    {
        auto delegate_description = delegate->getDataSourceDescription();
        delegate_description.is_encrypted = true;
        return delegate_description;
    }

    bool isRemote() const override { return delegate->isRemote(); }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    std::shared_ptr<DiskEncryptedTransaction> createEncryptedTransaction() const
    {
        auto delegate_transaction = delegate->createTransaction();
        return std::make_shared<DiskEncryptedTransaction>(delegate_transaction, disk_path, *current_settings.get(), delegate.get());
    }

    DiskTransactionPtr createTransaction() override
    {
        if (use_fake_transaction)
        {
            return std::make_shared<FakeDiskTransaction>(*this);
        }

        return createEncryptedTransaction();
    }

    std::optional<UInt64> getTotalSpace() const override
    {
        return delegate->getTotalSpace();
    }

    std::optional<UInt64> getAvailableSpace() const override
    {
        return delegate->getAvailableSpace();
    }

    std::optional<UInt64> getUnreservedSpace() const override
    {
        return delegate->getUnreservedSpace();
    }

    bool supportZeroCopyReplication() const override
    {
        return delegate->supportZeroCopyReplication();
    }

    MetadataStoragePtr getMetadataStorage() override
    {
        return delegate->getMetadataStorage();
    }

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & paths) const override;

    DiskPtr getDelegateDiskIfExists() const override
    {
        return delegate;
    }

    UInt32 getRefCount(const String & path) const override
    {
        auto wrapped_path = wrappedPath(path);
        return delegate->getRefCount(wrapped_path);
    }

#if USE_AWS_S3
    std::shared_ptr<const S3::Client> getS3StorageClient() const override
    {
        return delegate->getS3StorageClient();
    }

    std::shared_ptr<const S3::Client> tryGetS3StorageClient() const override { return delegate->tryGetS3StorageClient(); }
#endif

private:
    String wrappedPath(const String & path) const
    {
        return DiskEncryptedTransaction::wrappedPath(disk_path, path);
    }

    DiskPtr delegate;
    const String encrypted_name;
    const String disk_path;
    const String disk_absolute_path;
    MultiVersion<DiskEncryptedSettings> current_settings;
    bool use_fake_transaction;
};

}

#endif
