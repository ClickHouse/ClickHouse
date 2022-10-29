#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ReadOnlyMetadataStorage;

/// Transaction for read-only storage, throws NOT_IMPLEMENTED error.
/// Can be used to add limited read-only support of MergeTree.
class ReadOnlyMetadataTransaction : public IMetadataTransaction
{
public:
    void commit() override
    {
        /// Noop, nothing to commit.
    }

    void createEmptyMetadataFile(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void createMetadataFile(const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */) override
    {
        throwNotAllowed();
    }
    void writeStringToFile(const std::string & /* path */, const std::string & /* data */) override
    {
        throwNotAllowed();
    }
    void setLastModified(const std::string & /* path */, const Poco::Timestamp & /* timestamp */) override
    {
        throwNotAllowed();
    }
    void chmod(const String & /* path */, mode_t /* mode */) override
    {
        throwNotAllowed();
    }
    void setReadOnly(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void unlinkFile(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void removeDirectory(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void removeRecursive(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void createHardLink(const std::string & /* path_from */, const std::string & /* path_to */) override
    {
        throwNotAllowed();
    }
    void moveFile(const std::string & /* path_from */, const std::string & /* path_to */) override
    {
        throwNotAllowed();
    }
    void moveDirectory(const std::string & /* path_from */, const std::string & /* path_to */) override
    {
        throwNotAllowed();
    }
    void replaceFile(const std::string & /* path_from */, const std::string & /* path_to */) override
    {
        throwNotAllowed();
    }
    void createDirectory(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void createDirectoryRecursive(const std::string & /* path */) override
    {
        throwNotAllowed();
    }
    void addBlobToMetadata(const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */) override
    {
        throwNotAllowed();
    }
    void unlinkMetadata(const std::string & /* path */) override
    {
        throwNotAllowed();
    }

    bool supportsChmod() const override { return false; }

private:
    [[noreturn]] static void throwNotAllowed()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transaction for read-only storage is not supported");
    }
};

/// Readonly storage, throws NOT_IMPLEMENTED error.
/// Can be used to add limited read-only support of MergeTree.
class ReadOnlyMetadataStorage : public IMetadataStorage
{
public:
    Poco::Timestamp getLastModified(const std::string & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }
    uint32_t getHardlinkCount(const std::string & /* path */) const override
    {
        return 1;
    }

    struct stat stat(const String & /* path */) const override
    {
        throwNotImplemented();
    }
    time_t getLastChanged(const std::string & /* path */) const override
    {
        throwNotImplemented();
    }
    std::string readFileToString(const std::string & /* path */) const override
    {
        throwNotImplemented();
    }
    std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & /* file_paths */) const override
    {
        throwNotImplemented();
    }

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }

private:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is ont implemented");
    }
};

}
