#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <Poco/Timestamp.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/WriteMode.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

class IMetadataStorage;

/// Tries to provide some "transactions" interface, which allow
/// to execute (commit) operations simultaneously. We don't provide
/// any snapshot isolation here, so no read operations in transactions
/// interface. This transaction is more like "batch operation" than real "transaction".
///
/// But for better usability we can get MetadataStorage interface and use some read methods.
struct IMetadataTransaction : private boost::noncopyable
{
public:
    virtual void commit() = 0;

    virtual const IMetadataStorage & getStorageForNonTransactionalReads() const = 0;

    /// General purpose methods

    /// Write metadata string to file
    virtual void writeStringToFile(const std::string & path, const std::string & data) = 0;

    virtual void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) = 0;

    virtual void setReadOnly(const std::string & path) = 0;

    virtual void unlinkFile(const std::string & path) = 0;

    virtual void createDirectory(const std::string & path) = 0;

    virtual void createDicrectoryRecursive(const std::string & path) = 0;

    virtual void removeDirectory(const std::string & path) = 0;

    virtual void removeRecursive(const std::string & path) = 0;

    virtual void createHardLink(const std::string & path_from, const std::string & path_to) = 0;

    virtual void moveFile(const std::string & path_from, const std::string & path_to) = 0;

    virtual void moveDirectory(const std::string & path_from, const std::string & path_to) = 0;

    virtual void replaceFile(const std::string & path_from, const std::string & path_to) = 0;

    /// Metadata related methods

    /// Create empty file in metadata storage
    virtual void createEmptyMetadataFile(const std::string & path) = 0;

    /// Create metadata file on paths with content (blob_name, size_in_bytes)
    virtual void createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) = 0;

    /// Add to new blob to metadata file (way to implement appends)
    virtual void addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) = 0;

    /// Unlink metadata file and do something special if required
    /// By default just remove file (unlink file).
    virtual void unlinkMetadata(const std::string & path)
    {
        unlinkFile(path);
    }

    virtual ~IMetadataTransaction() = default;
};

using MetadataTransactionPtr = std::shared_ptr<IMetadataTransaction>;

/// Metadata storage for remote disks like DiskObjectStorage.
/// Support some subset of Disk operations, allow to read/write only
/// small amounts of data (strings).
class IMetadataStorage : private boost::noncopyable
{
public:
    virtual MetadataTransactionPtr createTransaction() const = 0;

    /// General purpose functions (similar to Disk)
    virtual const std::string & getPath() const = 0;

    virtual bool exists(const std::string & path) const = 0;

    virtual bool isFile(const std::string & path) const = 0;

    virtual bool isDirectory(const std::string & path) const = 0;

    virtual uint64_t getFileSize(const std::string & path) const = 0;

    virtual Poco::Timestamp getLastModified(const std::string & path) const = 0;

    virtual time_t getLastChanged(const std::string & path) const = 0;

    virtual std::vector<std::string> listDirectory(const std::string & path) const = 0;

    virtual DirectoryIteratorPtr iterateDirectory(const std::string & path) const = 0;

    virtual uint32_t getHardlinkCount(const std::string & path) const = 0;

    /// Read metadata file to string from path
    virtual std::string readFileToString(const std::string & path) const = 0;

    virtual ~IMetadataStorage() = default;

    /// ==== More specefic methods. Previous were almost general purpose. ====

    /// Read multiple metadata files into strings and return mapping from file_path -> metadata
    virtual std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & file_paths) const = 0;

    /// Return [(object_storage_path, size_in_bytes), ...] for metadata path
    /// object_storage_path is a full path to the blob.
    virtual PathsWithSize getObjectStoragePaths(const std::string & path) const = 0;
};

using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
