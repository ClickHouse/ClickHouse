#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include "Common/Exception.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class DiskBackup : public IDisk
{
private:
[[noreturn]] void throwNotAllowed() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only read-only operations are supported in DiskBackup");
}

public:
    DiskBackup(const String & name_, DiskPtr delegate_, MetadataStoragePtr metadata_);
    DiskBackup(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_);

    bool isReadOnly() const override
    {
        return true;
    }

    const String & getPath() const override
    {
        return delegate->getPath();
    }

    ReservationPtr reserve(UInt64 /* bytes */) override
    {
        throwNotAllowed();
    }

    std::optional<UInt64> getTotalSpace() const override
    {
        return delegate->getTotalSpace();
    }
    std::optional<UInt64> getAvailableSpace() const override
    {
        return {};
    }
    std::optional<UInt64> getUnreservedSpace() const override
    {
        return {};
    }

    UInt64 getKeepingFreeSpace() const override
    {
        throwNotAllowed();
    }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & /* path */) override
    {
        throwNotAllowed();
    }

    void createDirectories(const String & /* path */) override
    {
        throwNotAllowed();
    }

    void clearDirectory(const String & /* path */) override
    {
        throwNotAllowed();
    }

    void moveDirectory(const String & /* from_path */, const String & /* to_path */) override
    {
        throwNotAllowed();
    }

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    void createFile(const String & /* path */) override
    {
        throwNotAllowed();
    }

    void moveFile(const String & /* from_path */, const String & /* to_path */) override
    {
        throwNotAllowed();
    }

    void replaceFile(const String & /* from_path */, const String & /* to_path */) override
    {
        throwNotAllowed();
    }

    void listFiles(const String & path, std::vector<String> & file_names) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override
    {
        return delegate->readFile(path, settings, read_hint, file_size);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & /* path */,
        size_t /* buf_size */,
        WriteMode /* mode */,
        const WriteSettings & /* settings */) override
    {
        throwNotAllowed();
    }

    Strings getBlobPath(const String &  path) const override
    {
        return delegate->getBlobPath(path);
    }

    void writeFileUsingBlobWritingFunction(const String &  /*path*/, WriteMode  /*mode*/, WriteBlobFunction &&  /*write_blob_function*/) override
    {
        throwNotAllowed();
    }

    void removeFile(const String & /* path */) override
    {
        throwNotAllowed();
    }
    void removeFileIfExists(const String & /* path */) override
    {
        throwNotAllowed();
    }
    void removeDirectory(const String & /* path */) override
    {
        throwNotAllowed();
    }
    void removeRecursive(const String & /* path */) override
    {
        throwNotAllowed();
    }


    void setLastModified(const String &  /*path*/, const Poco::Timestamp &  /*timestamp*/) override
    {
        throwNotAllowed();
    }

    Poco::Timestamp getLastModified(const String &  path) const override
    {
        return delegate->getLastModified(path);
    }

    time_t getLastChanged(const String &  path) const override
    {
        return delegate->getLastChanged(path);
    }

    void setReadOnly(const String &  /*path*/) override
    {
        throwNotAllowed();
    }

    void createHardLink(const String &  /*src_path*/, const String &  /*dst_path*/) override
    {
        throwNotAllowed();
    }

    DataSourceDescription getDataSourceDescription() const override
    {
        return delegate->getDataSourceDescription();
    }

    /// Involves network interaction.
    bool isRemote() const override
    {
        return delegate->isRemote();
    }

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    bool supportZeroCopyReplication() const override
    {
        return delegate->supportZeroCopyReplication();
    }


private:
    DiskPtr delegate;
    MetadataStoragePtr metadata;
};

};


