#pragma once

#include "config.h"

#if USE_SSL

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <string>

namespace DB
{

class MetadataStorageWithPathWrapperTransaction final : public IMetadataTransaction
{
private:
    MetadataTransactionPtr delegate;
    std::string metadata_path;

    String wrappedPath(const String & path) const { return wrappedPath(metadata_path, path); }

public:
    static String wrappedPath(const String & path_wrapper, const String & path)
    {
        // if path starts_with metadata_path -> got already wrapped path
        if (!path_wrapper.empty() && path.starts_with(path_wrapper))
            return path;
        return path_wrapper + path;
    }

    MetadataStorageWithPathWrapperTransaction(MetadataTransactionPtr delegate_, const std::string & metadata_path_)
        : delegate(std::move(delegate_))
        , metadata_path(metadata_path_)
    {
    }

    ~MetadataStorageWithPathWrapperTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final
    {
        return delegate->getStorageForNonTransactionalReads();
    }

    void commit(const TransactionCommitOptionsVariant & options) final
    {
        delegate->commit(options);
    }

    void writeStringToFile(const std::string & path, const std::string & data) override
    {
        delegate->writeStringToFile(wrappedPath(path), data);
    }

    void writeInlineDataToFile(const std::string & path, const std::string & data) override
    {
        delegate->writeInlineDataToFile(wrappedPath(path), data);
    }

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override
    {
        delegate->createMetadataFile(wrappedPath(path), objects);
    }

    void addBlobToMetadata(const std::string & path, const StoredObject & object) override
    {
        delegate->addBlobToMetadata(wrappedPath(path), object);
    }

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override
    {
        delegate->setLastModified(wrappedPath(path), timestamp);
    }

    bool supportsChmod() const override { return delegate->supportsChmod(); }

    void chmod(const String & path, mode_t mode) override
    {
        delegate->chmod(wrappedPath(path), mode);
    }

    void setReadOnly(const std::string & path) override
    {
        delegate->setReadOnly(wrappedPath(path));
    }

    void unlinkFile(const std::string & path) override
    {
        delegate->unlinkFile(wrappedPath(path));
    }

    void createDirectory(const std::string & path) override
    {
        delegate->createDirectory(wrappedPath(path));
    }

    void createDirectoryRecursive(const std::string & path) override
    {
        delegate->createDirectoryRecursive(wrappedPath(path));
    }

    void removeDirectory(const std::string & path) override
    {
        delegate->removeDirectory(wrappedPath(path));
    }

    void removeRecursive(const std::string & path) override
    {
        delegate->removeRecursive(wrappedPath(path));
    }

    void createHardLink(const std::string & path_from, const std::string & path_to) override
    {
        delegate->createHardLink(wrappedPath(path_from), wrappedPath(path_to));
    }

    void moveFile(const std::string & path_from, const std::string & path_to) override
    {
        delegate->moveFile(wrappedPath(path_from), wrappedPath(path_to));
    }

    void moveDirectory(const std::string & path_from, const std::string & path_to) override
    {
        delegate->moveDirectory(wrappedPath(path_from), wrappedPath(path_to));
    }

    void replaceFile(const std::string & path_from, const std::string & path_to) override
    {
        delegate->replaceFile(wrappedPath(path_from), wrappedPath(path_to));
    }

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override
    {
        return delegate->unlinkMetadata(wrappedPath(path));
    }

    TruncateFileOperationOutcomePtr truncateFile(const std::string & src_path, size_t size) override
    {
        return delegate->truncateFile(wrappedPath(src_path), size);
    }

    std::optional<StoredObjects> tryGetBlobsFromTransactionIfExists(const std::string & path) const override
    {
        return delegate->tryGetBlobsFromTransactionIfExists(path);
    }
};

class MetadataStorageWithPathWrapper final : public IMetadataStorage
{
private:
    MetadataStoragePtr delegate;
    std::string metadata_path;
    std::string metadata_absolute_path;

    String wrappedPath(const String & path) const { return MetadataStorageWithPathWrapperTransaction::wrappedPath(metadata_path, path); }

public:
    MetadataStorageWithPathWrapper(MetadataStoragePtr delegate_, const std::string & metadata_path_)
        : delegate(std::move(delegate_))
        , metadata_path(metadata_path_)
        , metadata_absolute_path(wrappedPath(delegate->getPath()))
    {
    }

    MetadataTransactionPtr createTransaction() override
    {
        return std::make_shared<MetadataStorageWithPathWrapperTransaction>(delegate->createTransaction(), metadata_path);
    }

    const std::string & getPath() const override
    {
        return metadata_absolute_path;
    }

    MetadataStorageType getType() const override { return delegate->getType(); }

    /// Metadata on disk for an empty file can store empty list of blobs and size=0
    bool supportsEmptyFilesWithoutBlobs() const override { return delegate->supportsEmptyFilesWithoutBlobs(); }

    bool existsFile(const std::string & path) const override
    {
        return delegate->existsFile(wrappedPath(path));
    }

    bool existsDirectory(const std::string & path) const override
    {
        return delegate->existsDirectory(wrappedPath(path));
    }

    bool existsFileOrDirectory(const std::string & path) const override
    {
        return delegate->existsFileOrDirectory(wrappedPath(path));
    }

    uint64_t getFileSize(const String & path) const override
    {
        return delegate->getFileSize(wrappedPath(path));
    }

    Poco::Timestamp getLastModified(const std::string & path) const override
    {
        return delegate->getLastModified(wrappedPath(path));
    }

    time_t getLastChanged(const std::string & path) const override
    {
        return delegate->getLastChanged(wrappedPath(path));
    }

    bool supportsChmod() const override { return delegate->supportsChmod(); }

    bool supportsStat() const override { return delegate->supportsStat(); }

    struct stat stat(const String & path) const override { return delegate->stat(wrappedPath(path)); }

    std::vector<std::string> listDirectory(const std::string & path) const override
    {
        return delegate->listDirectory(wrappedPath(path));
    }

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override
    {
        return delegate->iterateDirectory(wrappedPath(path));
    }

    std::string readFileToString(const std::string & path) const override
    {
        return delegate->readFileToString(wrappedPath(path));
    }

    std::string readInlineDataToString(const std::string & path) const override
    {
        return delegate->readInlineDataToString(wrappedPath(path));
    }

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override
    {
        std::vector<String> wrapped_paths;
        wrapped_paths.reserve(file_paths.size());
        for (const auto & path : file_paths)
            wrapped_paths.push_back(wrappedPath(path));
        return delegate->getSerializedMetadata(wrapped_paths);
    }

    uint32_t getHardlinkCount(const std::string & path) const override
    {
        return delegate->getHardlinkCount(wrappedPath(path));
    }

    StoredObjects getStorageObjects(const std::string & path) const override
    {
        return delegate->getStorageObjects(wrappedPath(path));
    }

    bool isReadOnly() const override
    {
        return delegate->isReadOnly();
    }
};


}

#endif
