#pragma once
#include "config.h"
#if USE_AVRO

#include <atomic>
#include <cstddef>
#include <deque>
#include <future>
#include <memory>
#include <optional>


#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

#include <boost/noncopyable.hpp>

namespace DB::Iceberg
{

/// Reads the manifest file without parsing it; the parse happens on the first `getDeserializer` call.
Iceberg::ManifestFileCacheableInfo getManifestFile(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const IcebergPathFromMetadata & filename,
    ExternalStorageCache & external_storages);

/// Creates a fully initialized ManifestFileIterator from a cache key.
/// All entries are drained so that aggregate methods (e.g. getRowsCountInAllFilesExcludingDeleted)
/// can be called on the returned iterator.
Iceberg::ManifestFileIterator::ManifestFileEntriesHandle getManifestFileEntriesHandle(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const ManifestFileCacheKey & cache_key,
    Int32 table_snapshot_schema_id,
    ExternalStorageCache & external_storages);

Iceberg::ManifestFileIterator::ManifestFileEntriesHandle getManifestFileEntriesHandle(
    const ManifestFileCacheableInfo & manifest_file,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    const ManifestFileCacheKey & cache_key,
    Int32 table_snapshot_schema_id);


ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    const IcebergPathFromMetadata & filename,
    LoggerPtr log,
    ExternalStorageCache & external_storages);

/// Reads manifest files ahead, in order, on the IO thread pool. Not thread-safe.
class ManifestFilesPrefetcher : private boost::noncopyable
{
public:
    /// A manifest file that is read or being read. Holds one of the prefetcher's slots until destroyed,
    /// so it must not outlive the prefetcher.
    class Fetch : private boost::noncopyable
    {
    public:
        ~Fetch();

        const ManifestFileCacheKey & key() const { return manifest; }

        /// Waits for the read. Must be called at most once.
        ManifestFileCacheableInfo get();

    private:
        friend class ManifestFilesPrefetcher;

        Fetch(ManifestFilesPrefetcher & owner_, const ManifestFileCacheKey & manifest_, size_t leased_bytes_);

        ManifestFilesPrefetcher & owner;
        const ManifestFileCacheKey & manifest;
        const size_t leased_bytes;
        std::optional<ManifestFileCacheableInfo> cached;
        std::future<ManifestFileCacheableInfo> pending;
    };

    using FetchPtr = std::unique_ptr<Fetch>;

    ManifestFilesPrefetcher(
        ObjectStoragePtr object_storage_,
        const PersistentTableComponents & persistent_table_components_,
        ContextPtr local_context_,
        LoggerPtr log_,
        ManifestFileCacheKeys manifests_,
        std::shared_ptr<ExternalStorageCache> external_storages_);

    ~ManifestFilesPrefetcher();

    /// The next manifest file in order. Returns nullptr when none is left, and also while all slots are
    /// held by taken fetches: then it succeeds again once one of them is destroyed.
    FetchPtr take();

private:
    void fill();

    static constexpr size_t max_manifest_files_in_flight = 64;
    /// Counts only the files that are not in the metadata files cache. A larger file is still read when it is the only one.
    static constexpr size_t max_manifest_bytes_in_flight = 64 << 20;

    const ObjectStoragePtr object_storage;
    const PersistentTableComponents persistent_table_components;
    const ContextPtr local_context;
    const LoggerPtr log;
    const ManifestFileCacheKeys manifests;
    const std::shared_ptr<ExternalStorageCache> external_storages;
    const bool use_metadata_cache;

    size_t next_to_fetch = 0;
    /// Released by `Fetch` destructors, which may run outside the owner's lock.
    std::atomic<size_t> leased = 0;
    std::atomic<size_t> leased_bytes = 0;
    /// Set on destruction: the reads that have not started yet are skipped.
    const std::shared_ptr<std::atomic<bool>> stopped = std::make_shared<std::atomic<bool>>(false);
    std::deque<FetchPtr> window;
};

}

#endif
