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
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/Location.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/DiskType.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct IDiskTransaction;

/// Tries to provide some "transactions" interface, which allow
/// to execute (commit) operations simultaneously. We don't provide
/// any snapshot isolation here, so no read operations in transactions
/// interface. This transaction is more like "batch operation" than real "transaction".
///
/// But for better usability we can get MetadataStorage interface and use some read methods.
class IMetadataTransaction : private boost::noncopyable
{
public:
    virtual void commit(const TransactionCommitOptionsVariant & options) = 0;
    virtual TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) = 0;

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

    virtual void unlinkFile(const std::string & /* path */, bool /* if_exists */, bool /* should_remove_objects */)
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

    using ShouldRemoveObjectsPredicate = std::function<bool(const std::string & relative_path)>;
    virtual void removeRecursive(const std::string & /* path */, const ShouldRemoveObjectsPredicate & /* should_remove_objects */)
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

    /// [TXN-ONE-PIPELINE] Optional per-metadata write buffer. Returns a ready-to-use buffer when the
    /// metadata implementation owns its write mechanism (e.g. a content-addressed hash-on-write buffer
    /// whose blob key is known only after the last byte). `owner` is the disk transaction that must be
    /// kept alive for the returned buffer's lifetime and, when `autocommit`, committed from the finalize
    /// callback. Default nullptr: the caller uses the generic streaming write path unchanged.
    virtual std::unique_ptr<WriteBufferFromFileBase> tryCreateWriteBuffer(
        const std::shared_ptr<IDiskTransaction> & /*owner*/,
        const std::string & /*path*/, size_t /*buf_size*/, WriteMode /*mode*/,
        const WriteSettings & /*settings*/, bool /*autocommit*/) { return nullptr; }

    /// Metadata related methods

    /// Generate blob name for passed absolute local path.
    /// Path can be generated either independently or based on `path`.
    virtual ObjectStorageKey generateObjectKeyForPath(const std::string & path) = 0;
    virtual void recordBlobsReplication(const StoredObject & /*blob*/, const Locations & missing_locations)
    {
        if (!missing_locations.empty())
            throwNotImplemented();
    }
    virtual StoredObjects getSubmittedForRemovalBlobs() = 0;

    /// Create metadata file on paths with content consisting of objects
    virtual void createMetadataFile(const std::string & path, const StoredObjects & objects) = 0;

    /// Add to new blob to metadata file (way to implement appends).
    virtual void addBlobToMetadata(const std::string & /* path */, const StoredObject & /* object */)
    {
        throwNotImplemented();
    }

    virtual void truncateFile(const std::string & /* path */, size_t /* size */)
    {
        throwNotImplemented();
    }

    /// In-flight read-your-writes for a part being assembled by THIS transaction (B59). A CA part-build
    /// transaction stages blobs (uploaded) + mutable bytes before the single commit; these let a reader
    /// that holds the transaction resolve those staged files before they are committed. Default: no
    /// in-flight visibility (the committed metadata path is authoritative).
    virtual std::optional<StoredObjects> tryGetInFlightStorageObjects(const std::string & /*path*/) const { return {}; }
    virtual std::unique_ptr<ReadBufferFromFileBase> tryReadFileInFlight(
        const std::string & /*path*/, const ReadSettings & /*settings*/, std::optional<size_t> /*read_hint*/) const { return nullptr; }
    virtual std::optional<uint64_t> tryGetInFlightFileSize(const std::string & /*path*/) const { return {}; }
    /// Directory-granularity counterpart of the file trio: true iff this transaction has STAGED at least one
    /// file under `path` for `path`'s part. Used so a carried-forward projection dir is visible to
    /// loadProjections during finalize. Default: no in-flight directory visibility.
    virtual bool hasInFlightDirectory(const std::string & /*path*/) const { return false; }
    /// Immediate-child names staged directly under `path` (one level). Used so loadProjections'
    /// withPartFormatFromDisk can iterate a staged projection dir to find its mark file. Default: empty.
    virtual std::vector<std::string> listInFlightDirectory(const std::string & /*path*/) const { return {}; }

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

    virtual std::string getZooKeeperName() const { return ""; }
    virtual std::string getZooKeeperPath() const { return ""; }

    /// Returns true if empty file can be created without any blobs in the corresponding object storage.
    /// E.g. metadata storage can store the empty list of blobs corresponding to a file without actually storing any blobs.
    /// But if the metadata storage just relies on for example local FS to store data under logical path, then a file has to be created even if it's empty.
    virtual bool supportsEmptyFilesWithoutBlobs() const { return false; }

    /// Returns true if underlying blob ids generator uses random.
    virtual bool areBlobPathsRandom() const = 0;

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

    virtual void startup() {}
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

    virtual bool isReadOnly() const = 0;

    virtual bool isTransactional() const
    {
        return false;
    }

    virtual bool isPlain() const
    {
        return false;
    }

    virtual bool isWriteOnce() const
    {
        return false;
    }

    /// Returns true if the metadata storage is content-addressed, i.e. blob keys are derived
    /// from content hashes and are only known after all bytes have been written. Such a storage
    /// cannot use the up-front-key streaming write path of `DiskObjectStorageTransaction`; the
    /// disk transaction delegates writes to the metadata transaction's content-addressed buffer.
    virtual bool isContentAddressed() const { return false; }

    /// [TXN-ONE-PIPELINE] True when a transaction from this storage stages every mutation into a
    /// transaction-private overlay at call time (eager) rather than queuing effects for FIFO replay in
    /// commit. When true, DiskObjectStorageTransaction routes every mutating method straight to the
    /// metadata transaction and keeps its own operations_to_execute queue empty. Default false
    /// (ordinary object storage).
    virtual bool transactionIsStagingOverlay() const { return false; }

    /// True when a file write through this metadata storage publishes atomically, i.e. no partial
    /// content is ever observable under the file's final name (see `IDataPartStorage::supportsAtomicFileWrites`).
    virtual bool supportsAtomicFileWrites() const { return false; }

    using BlobsToRemove = std::unordered_map<StoredObject, LocationSet>;
    virtual BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & /*cluster*/, int64_t /*max_count*/) { return {}; }
    virtual int64_t recordAsRemoved(const StoredObjects & /*blobs*/) { return 0; }
    virtual bool hasPendingRemovalBlobs(const StoredObjects & /*blobs*/) const { return false; }

    struct BlobsReplication
    {
        StoredObject blob;
        Location from;
        Location to;
    };
    using BlobsToReplicate = std::vector<BlobsReplication>;
    virtual BlobsToReplicate getBlobsToReplicate(const ClusterConfigurationPtr & /*cluster*/, int64_t /*max_count*/) { return {}; }
    virtual int64_t recordAsReplicated(const BlobsToReplicate & /*blobs*/) { return 0; }
    virtual bool hasUnreplicatedBlobs(const Location & /*location_to_check*/) { return false; }

    /// Re-read paths or their full subtrees from disk and update cache.
    /// Can return serialized description of cache update which can be used to populate cache on other nodes.
    virtual void updateCache(const std::vector<std::string> & /* paths */, bool /* recursive */, bool /* enforce_fresh */,
        std::string * /* serialized_cache_update_description */) {}
    /// Allows to apply cache update from serialized description.
    virtual void updateCacheFromSerializedDescription(const std::string & /* serialized_cache_update_description */) {}
    virtual void invalidateCache(const std::string & /* path */) {}

    /// Clear all cache content.
    virtual void dropCache() {}

    /// Apply configuration changes.
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        ContextPtr /* context */) {}

    /// True if write with Append mode supported.
    virtual bool supportWritingWithAppend() const { return false; }

    /// True iff this metadata storage can persist the per-part mutable transaction file (txn_version.txt)
    /// under MVCC. Distinct from supportWritingWithAppend: transactions rewrite txn_version.txt (tmp +
    /// replaceFile), they never WriteMode::Append, so append-capability is the wrong proxy. A
    /// content-addressed disk supports the mutable txn file via its per-ref sidecar.
    virtual bool supportsTransactionalMutableFiles() const { return false; }

protected:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is not implemented");
    }
};

using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
