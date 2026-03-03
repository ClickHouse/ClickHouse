#pragma once
#include "config.h"

#include <filesystem>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/SettingsChanges.h>

namespace fs = std::filesystem;
namespace Poco { class Logger; }

namespace DB
{
class StorageObjectStorageQueue;
struct ObjectStorageQueueSettings;
struct ObjectStorageQueueTableMetadata;
struct StorageInMemoryMetadata;

/**
 * A class for managing ObjectStorageQueue metadata in zookeeper, e.g.
 * the following folders:
 * - <path_to_metadata>/processed
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * In case we use buckets for processing for Ordered mode, the structure looks like:
 * - <path_to_metadata>/buckets/<bucket>/processed -- persistent node, information about last processed file.
 * - <path_to_metadata>/buckets/<bucket>/lock -- ephemeral node, used for acquiring bucket lock.
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * Depending on ObjectStorageQueue processing mode (ordered or unordered)
 * we can differently store metadata in /processed node.
 *
 * Implements caching of zookeeper metadata for faster responses.
 * Cached part is located in LocalFileStatuses.
 *
 * In case of Unordered mode - if files TTL is enabled or maximum tracked files limit is set
 * starts a background cleanup thread which is responsible for maintaining them.
 */
class ObjectStorageQueueMetadata
{
public:
    using FileStatus = ObjectStorageQueueIFileMetadata::FileStatus;
    using FileMetadataPtr = std::shared_ptr<ObjectStorageQueueIFileMetadata>;
    using FileStatusPtr = std::shared_ptr<FileStatus>;
    using FileStatuses = std::unordered_map<std::string, FileStatusPtr>;
    using Bucket = size_t;
    using Processor = std::string;

    ObjectStorageQueueMetadata(
        ObjectStorageType storage_type_,
        const fs::path & zookeeper_path_,
        const ObjectStorageQueueTableMetadata & table_metadata_,
        size_t cleanup_interval_min_ms_,
        size_t cleanup_interval_max_ms_,
        bool use_persistent_processing_nodes_,
        size_t persistent_processing_nodes_ttl_seconds_,
        size_t keeper_multiread_batch_size_);

    ~ObjectStorageQueueMetadata();

    /// Startup background threads.
    void startup();
    /// Shutdown background threads.
    void shutdown();

    /// Check if metadata in keeper by `zookeeper_path` exists.
    /// If it does not exist - create it.
    /// If it already exists:
    /// - check that passed queue settings, columns, format are consistent
    /// with existing metadata in keeper;
    /// - adjust, if needed and allowed, passed queue settings according to
    /// what is in keeper (for example, processing_threads_num is adjustable,
    /// because its default depends on the CPU cores on the server);
    static ObjectStorageQueueTableMetadata syncWithKeeper(
        const fs::path & zookeeper_path,
        const ObjectStorageQueueSettings & settings,
        const ColumnsDescription & columns,
        const std::string & format,
        const ContextPtr & context,
        bool is_attach,
        LoggerPtr log);
    /// Alter settings in keeper metadata
    /// (rewrites what we write in syncWithKeeper()).
    void alterSettings(const SettingsChanges & changes, const ContextPtr & context);

    /// Get object storage type: s3, azure, local, etc.
    ObjectStorageType getType() const { return storage_type; }
    /// Get base path to keeper metadata.
    std::string getPath() const { return zookeeper_path; }
    /// Get statuses (state, processed rows, processing time)
    /// of all files stored in LocalFileStatuses cache.
    FileStatuses getFileStatuses() const;

    /// Get TableMetadata, which is the exact information we store in keeper.
    const ObjectStorageQueueTableMetadata & getTableMetadata() const { return table_metadata; }
    ObjectStorageQueueTableMetadata & getTableMetadata() { return table_metadata; }

    /// Create ObjectStorageQueueIFileMetadata object
    /// for the requested file.
    /// ObjectStorageQueueIFileMetadata (either Ordered or Unordered implementation)
    /// allows to manage metadata of a concrete file.
    FileMetadataPtr getFileMetadata(
        const std::string & path,
        ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info = {});

    /// Register table in keeper metadata.
    /// active = false:
    ///     On each CREATE TABLE query we register it persistently in keeper
    ///     under a persistent node "zookeeper_path / registry".
    ///     This is needed to be able to know when we would have to delete all metadata in keeper.
    ///     Metadata can be deleted only by the last registered table.
    ///     FIXME: actually a race condition is possible here
    ///     (when we checked that we are the last table and started deleting the metadata
    ///     while someone else registered after we checked :/ )
    ///
    /// active = true:
    ///     We also want to register nodes only for a period when they are active.
    ///     For this we create ephemeral nodes in "zookeeper_path / registry / <node_info>"
    void registerNonActive(const StorageID & storage_id, bool & created_new_metadata);
    void registerActive(const StorageID & storage_id);

    /// Unregister table.
    /// Return the number of remaining (after unregistering) registered tables.
    void unregisterActive(const StorageID & storage_id);
    void unregisterNonActive(const StorageID & storage_id, bool remove_metadata_if_no_registered);
    Strings getRegistered(bool active);

    /// According to current *active* registered tables,
    /// check using a hash ring which table would process these paths.
    /// Leave only those paths which need to be processed by current table.
    void filterOutForProcessor(Strings & paths, const StorageID & storage_id) const;

    /// Method of Ordered mode parallel processing.
    bool useBucketsForProcessing() const;
    /// Get number of buckets in case of bucket-based processing.
    size_t getBucketsNum() const { return buckets_num; }
    /// Get bucket by file path in case of bucket-based processing.
    Bucket getBucketForPath(const std::string & path) const;
    /// Acquire (take unique ownership of) bucket for processing.
    ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr tryAcquireBucket(const Bucket & bucket);

    static std::shared_ptr<ZooKeeperWithFaultInjection> getZooKeeper(LoggerPtr log);
    static ZooKeeperRetriesControl getKeeperRetriesControl(LoggerPtr log);

    /// Set local ref count for metadata.
    void setMetadataRefCount(std::atomic<size_t> & ref_count_) { chassert(!metadata_ref_count); metadata_ref_count = &ref_count_; }

    void updateSettings(const SettingsChanges & changes);

    std::pair<size_t, size_t> getCleanupIntervalMS() const { return {cleanup_interval_min_ms, cleanup_interval_max_ms }; }

    bool usePersistentProcessingNode() const { return use_persistent_processing_nodes; }
    size_t getPersistentProcessingNodeTTLSeconds() const { return persistent_processing_node_ttl_seconds; }

private:
    void cleanupThreadFunc();
    void cleanupThreadFuncImpl();
    void cleanupPersistentProcessingNodes();

    void migrateToBucketsInKeeper(size_t value);

    void updateRegistryFunc();
    void updateRegistry(const DB::Strings & registered_);

    /// Get ID for the specified table which is used for active tables.
    static std::string getProcessorID(const StorageID & storage_id);

    ObjectStorageQueueTableMetadata table_metadata;
    const ObjectStorageType storage_type;
    const ObjectStorageQueueMode mode;
    const fs::path zookeeper_path;
    const size_t keeper_multiread_batch_size;

    std::atomic<size_t> cleanup_interval_min_ms;
    std::atomic<size_t> cleanup_interval_max_ms;
    std::atomic<bool> use_persistent_processing_nodes;
    std::atomic<size_t> persistent_processing_node_ttl_seconds;

    size_t buckets_num;
    std::unique_ptr<ThreadFromGlobalPool> update_registry_thread;

    LoggerPtr log;

    std::atomic_bool shutdown_called = false;
    std::atomic_bool startup_called = false;
    BackgroundSchedulePoolTaskHolder task;

    class LocalFileStatuses;
    std::shared_ptr<LocalFileStatuses> local_file_statuses;

    /// A set of currently known "active" servers.
    /// The set is updated by updateRegistryFunc().
    NameSet active_servers;
    /// Hash ring implementation.
    class ServersHashRing;
    /// Hash ring object.
    /// Can be updated by updateRegistryFunc(), when `active_servers` set changes.
    std::shared_ptr<ServersHashRing> active_servers_hash_ring;
    /// Guards `active_servers` and `active_servers_hash_ring`.
    mutable SharedMutex active_servers_mutex;

    /// Number of S3(Azure)Queue tables on the same
    /// clickhouse server instance referencing the same metadata object.
    std::atomic<size_t> * metadata_ref_count = nullptr;
};

using ObjectStorageQueueMetadataPtr = std::unique_ptr<ObjectStorageQueueMetadata>;

}
