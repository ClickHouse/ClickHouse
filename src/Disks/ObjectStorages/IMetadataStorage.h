#pragma once

#include <memory>
#include <optional>
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
#include <Disks/DiskType.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IMetadataStorage;
struct PartitionCommand;

/// Return the result of operation to the caller.
/// It is used in `IDiskObjectStorageOperation::finalize` after metadata transaction executed to make decision on blob removal.
struct UnlinkMetadataFileOperationOutcome
{
    UInt32 num_hardlinks = std::numeric_limits<UInt32>::max();
};

struct TruncateFileOperationOutcome
{
    StoredObjects objects_to_remove;
};


using UnlinkMetadataFileOperationOutcomePtr = std::shared_ptr<UnlinkMetadataFileOperationOutcome>;
using TruncateFileOperationOutcomePtr = std::shared_ptr<TruncateFileOperationOutcome>;


/// Tries to provide some "transactions" interface, which allow
/// to execute (commit) operations simultaneously. We don't provide
/// any snapshot isolation here, so no read operations in transactions
/// interface. This transaction is more like "batch operation" than real "transaction".
///
/// But for better usability we can get MetadataStorage interface and use some read methods.
class IMetadataTransaction : private boost::noncopyable
{
public:
    virtual void commit() = 0;

    virtual const IMetadataStorage & getStorageForNonTransactionalReads() const = 0;

    /// General purpose methods

    /// Write metadata string to file
    virtual void writeStringToFile(const std::string & /* path */, const std::string & /* data */)
    {
        throwNotImplemented();
    }

    /// Writes the data inline with the metadata
    virtual void writeInlineDataToFile(const std::string & /* path */, const std::string & /* data */)
    {
        throwNotImplemented();
    }

    virtual void setLastModified(const std::string & /* path */, const Poco::Timestamp & /* timestamp */)
    {
        throwNotImplemented();
    }

    virtual bool supportsChmod() const = 0;
    virtual void chmod(const String & /* path */, mode_t /* mode */)
    {
        throwNotImplemented();
    }

    virtual void setReadOnly(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void unlinkFile(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void createDirectory(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void createDirectoryRecursive(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void removeDirectory(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void removeRecursive(const std::string & /* path */)
    {
        throwNotImplemented();
    }

    virtual void createHardLink(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }

    virtual void moveFile(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }

    virtual void moveDirectory(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }

    virtual void replaceFile(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }

    /// Metadata related methods

    /// Create empty file in metadata storage
    virtual void createEmptyMetadataFile(const std::string & path) = 0;

    virtual void createEmptyFile(const std::string & /* path */) {}

    /// Create metadata file on paths with content (blob_name, size_in_bytes)
    virtual void createMetadataFile(const std::string & path, ObjectStorageKey key, uint64_t size_in_bytes) = 0;

    /// Add to new blob to metadata file (way to implement appends)
    virtual void addBlobToMetadata(const std::string & /* path */, ObjectStorageKey /* key */, uint64_t /* size_in_bytes */)
    {
        throwNotImplemented();
    }

    /// Unlink metadata file and do something special if required
    /// By default just remove file (unlink file).
    virtual UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path)
    {
        unlinkFile(path);
        return nullptr;
    }

    virtual TruncateFileOperationOutcomePtr truncateFile(const std::string & /* path */, size_t /* target_size */)
    {
        throwNotImplemented();
    }

    virtual ~IMetadataTransaction() = default;

protected:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is not implemented");
    }
};

using MetadataTransactionPtr = std::shared_ptr<IMetadataTransaction>;

/// Metadata storage for remote disks like DiskObjectStorage.
/// Support some subset of Disk operations, allow to read/write only
/// small amounts of data (strings).
class IMetadataStorage : private boost::noncopyable
{
public:
    virtual MetadataTransactionPtr createTransaction() = 0;

    /// Get metadata root path.
    virtual const std::string & getPath() const = 0;

    virtual MetadataStorageType getType() const = 0;

    /// Returns true if empty file can be created without any blobs in the corresponding object storage.
    /// E.g. metadata storage can store the empty list of blobs corresponding to a file without actually storing any blobs.
    /// But if the metadata storage just relies on for example local FS to store data under logical path, then a file has to be created even if it's empty.
    virtual bool supportsEmptyFilesWithoutBlobs() const { return false; }

    /// ==== General purpose methods. Define properties of object storage file based on metadata files ====

    virtual bool existsFile(const std::string & path) const = 0;
    virtual bool existsDirectory(const std::string & path) const = 0;
    virtual bool existsFileOrDirectory(const std::string & path) const = 0;

    virtual uint64_t getFileSize(const std::string & path) const = 0;

    virtual std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const
    {
        if (existsFile(path))
            return getFileSize(path);
        return std::nullopt;
    }

    virtual Poco::Timestamp getLastModified(const std::string & path) const = 0;

    virtual std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const
    {
        if (existsFileOrDirectory(path))
            return getLastModified(path);
        return std::nullopt;
    }

    virtual time_t getLastChanged(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    virtual bool supportsChmod() const = 0;

    virtual bool supportsStat() const = 0;
    virtual struct stat stat(const String & /* path */) const
    {
        throwNotImplemented();
    }

    virtual bool supportsPartitionCommand(const PartitionCommand & /* command */) const = 0;

    virtual std::vector<std::string> listDirectory(const std::string & path) const = 0;

    virtual DirectoryIteratorPtr iterateDirectory(const std::string & path) const = 0;

    virtual bool isDirectoryEmpty(const std::string & path) const
    {
        return !iterateDirectory(path)->isValid();
    }

    virtual uint32_t getHardlinkCount(const std::string & path) const = 0;

    /// Read metadata file to string from path
    virtual std::string readFileToString(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    /// Read inline data for file to string from path
    virtual std::string readInlineDataToString(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    virtual void shutdown()
    {
        /// This method is overridden for specific metadata implementations in ClickHouse Cloud.
    }

    /// If the state can be changed under the hood and become outdated in memory, perform a reload if necessary,
    /// but don't do it more frequently than the specified parameter.
    /// Note: for performance reasons, it's allowed to assume that only some subset of changes are possible
    /// (those that MergeTree tables can make).
    virtual void refresh(UInt64 /* not_sooner_than_milliseconds */)
    {
        /// The default no-op implementation when the state in memory cannot be out of sync of the actual state.
    }

    virtual ~IMetadataStorage() = default;

    /// ==== More specific methods. Previous were almost general purpose. ====

    /// Read multiple metadata files into strings and return mapping from file_path -> metadata
    virtual std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & /* file_paths */) const
    {
        throwNotImplemented();
    }

    /// Return object information (absolute_path, bytes_size, ...) for metadata path.
    /// object_storage_path is absolute.
    virtual StoredObjects getStorageObjects(const std::string & path) const = 0;

    virtual std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const
    {
        if (existsFile(path))
            return getStorageObjects(path);
        return std::nullopt;
    }

protected:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is not implemented");
    }
};

using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
