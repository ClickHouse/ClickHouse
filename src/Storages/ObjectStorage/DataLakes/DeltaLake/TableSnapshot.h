#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/Types.h>
#include <IO/S3/URI.h>
#include <Common/Logger.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <boost/noncopyable.hpp>
#include "delta_kernel_ffi.hpp"

#include <atomic>
#include <future>
#include <mutex>

namespace DeltaLake
{

/**
 * A class representing DeltaLake table snapshot -
 * a snapshot of table state, its schema, data files, etc.
 */
class TableSnapshot : public std::enable_shared_from_this<TableSnapshot>
{
public:
    static constexpr auto LATEST_SNAPSHOT_VERSION = -1;

    explicit TableSnapshot(
        std::optional<size_t> version_,
        KernelHelperPtr helper_,
        DB::ObjectStoragePtr object_storage_,
        LoggerPtr log_);

    /// Get snapshot version.
    size_t getVersion() const;

    /// True when nothing is installed yet and the only load on this snapshot was given up on by
    /// every waiter. The metadata layer then resolves the latest version through a fresh object
    /// rather than reusing this one, so that one object never resolves two different versions.
    bool isAbandonedWithoutWaiters() const;

    /// True once a kernel snapshot state is installed (the object is usable as a cache entry).
    bool isInitialized() const;

    /// False when a build is in flight — or reserved, before the first load started — with
    /// client options different from `client_options`: query-level S3 timeouts are forwarded
    /// into the build, so such a query must not adopt it.
    bool canShareInflightLoad(const KernelClientOptions & client_options) const;

    /// Records the options the first load of this object must use (see canShareInflightLoad).
    void reserveClientOptions(const KernelClientOptions & client_options);

    std::optional<size_t> getTotalRows() const;
    std::optional<size_t> getTotalBytes() const;

    /// Iterate over DeltaLake data files.
    DB::ObjectIterator iterate(
        const DB::ActionsDAG * filter_dag,
        DB::IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        DB::ContextPtr context);

    /// Get schema from DeltaLake table metadata.
    const DB::NamesAndTypesList & getTableSchema() const;
    /// Get read schema derived from data files.
    /// (In most cases it would be the same as table schema).
    const DB::NamesAndTypesList & getReadSchema() const;
    /// DeltaLake stores partition columns values not in the data files,
    /// but in data file path directory names.
    /// Therefore "table schema" would contain partition columns,
    /// but "read schema" would not.
    const DB::Names & getPartitionColumns() const;
    const DB::NameToNameMap & getPhysicalNamesMap() const;

    DB::ObjectStoragePtr getObjectStorage() const { return object_storage; }
private:
    class Iterator;

    using KernelExternEngine = KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelSnapshot = KernelPointerWrapper<ffi::SharedSnapshot, ffi::free_snapshot>;
    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanMetadataIterator = KernelPointerWrapper<ffi::SharedScanMetadataIterator, ffi::free_scan_metadata_iter>;
    using KernelDvInfo = KernelPointerWrapper<ffi::SharedDvInfo, ffi::free_kernel_dv_info>;
    using KernelExpression = KernelPointerWrapper<ffi::SharedExpression, ffi::free_kernel_expression>;

    using TableSchema = DB::NamesAndTypesList;
    using ReadSchema = DB::NamesAndTypesList;

    const KernelHelperPtr helper;
    const DB::ObjectStoragePtr object_storage;
    const LoggerPtr log;
    /// std::nullopt means latest version must be used
    const std::optional<size_t> snapshot_version_to_read;

    struct KernelSnapshotState : private boost::noncopyable
    {
        /// `used_credentials_fingerprint` receives the fingerprint of the credentials the engine
        /// is built with as soon as they are known — before the kernel calls which may throw —
        /// so that a failed build can still be judged against the credentials it actually used.
        KernelSnapshotState(
            const IKernelHelper & helper_,
            std::optional<size_t> snapshot_version_,
            const KernelClientOptions & client_options_,
            DB::UInt128 & used_credentials_fingerprint);

        KernelExternEngine engine;
        KernelSnapshot snapshot;
        KernelScan scan;
        size_t snapshot_version;
        /// Fingerprint of the credentials embedded into `engine`, taken when it was built.
        DB::UInt128 credentials_fingerprint{};
    };
    mutable std::shared_ptr<KernelSnapshotState> kernel_snapshot_state;
    mutable DB::UInt128 kernel_state_credentials_fingerprint{};
    /// Set after a credentials refresh whose fingerprint may not have changed, so that the next
    /// `initOrUpdateSnapshot` rebuilds the engine anyway (without ever leaving the state null).
    mutable bool kernel_state_needs_rebuild = false;

    /// One in-flight kernel snapshot build, shared between the worker thread that runs the
    /// kernel call and every query waiting for its result. Waiters poll the future outside
    /// `mutex`, each with its own cancellation checks.
    struct InflightSnapshotLoad : private boost::noncopyable
    {
        enum class State
        {
            Running,
            Finished,   /// Set by the worker when the kernel call returned.
            Abandoned,  /// Set by the first waiter that gave up (KILL QUERY or a timeout).
        };

        std::shared_future<std::shared_ptr<KernelSnapshotState>> future;
        std::atomic<State> state{State::Running};
        /// Queries currently waiting for this load, registered under `TableSnapshot::mutex` at
        /// adoption time. A load some waiter gave up on is still healthy work for the others;
        /// only a load nobody waits for is considered dead.
        std::atomic<Int64> waiters{0};
        /// Client options the build runs with; a query with different options does not share it.
        KernelClientOptions client_options;
        /// Fingerprint of the credentials the worker built (or tried to build) the engine with.
        /// Written by the worker before the future becomes ready, so readers which observed the
        /// ready future see it; the stale-token retry compares against it, not against whatever
        /// the helper's client is by the time some waiter handles the failure.
        DB::UInt128 credentials_fingerprint{};
    };
    mutable std::shared_ptr<InflightSnapshotLoad> inflight_load TSA_GUARDED_BY(mutex);
    /// Client options this object is meant to load with, recorded by the metadata layer before
    /// the object is published, so that a query with other options is turned away even before
    /// the first load has started (see canShareInflightLoad).
    mutable std::optional<KernelClientOptions> reserved_client_options TSA_GUARDED_BY(mutex);

    struct SchemaInfo
    {
        /// Table logical schema
        /// (e.g. actual table schema)
        TableSchema table_schema;
        /// Table read schema
        /// (contains only columns contained in data file,
        /// e.g. does not contain partition columns, generated columns, etc)
        ReadSchema read_schema;
        /// Mapping for physical names of parquet data files
        DB::NameToNameMap physical_names_map;
        /// Partition columns list (not stored in read schema)
        DB::Names partition_columns;
    };
    mutable std::optional<SchemaInfo> schema;

    struct SnapshotStats
    {
        /// Total number of bytes in table
        std::optional<size_t> total_bytes;
        /// Total number of rows in table
        std::optional<size_t> total_rows;
    };
    mutable std::optional<SnapshotStats> snapshot_stats;

    mutable std::mutex mutex;

    size_t getVersionUnlocked() const TSA_REQUIRES(mutex);

    /// Ensures `kernel_snapshot_state` is built and current. Acquires `mutex` itself and
    /// releases it while waiting for the in-flight build, so it must be called WITHOUT the
    /// lock held: every public entry point calls it first, then re-locks for its guarded reads.
    void initOrUpdateSnapshot() const TSA_NO_THREAD_SAFETY_ANALYSIS;
    void initOrUpdateSchemaIfChanged() const TSA_REQUIRES(mutex);

    /// Acquires `mutex` itself: a stale-credentials retry rebuilds the engine through
    /// `initOrUpdateSnapshot` (killable, mutex released) instead of blocking under the lock.
    SnapshotStats getSnapshotStats() const;
    SnapshotStats getSnapshotStatsImpl() const TSA_REQUIRES(mutex);

    /// One-shot recovery from `DELTA_KERNEL_ERROR` with `ExpiredToken`/`InvalidToken`:
    /// invokes the catalog refresh callback and compares pre/post credentials fingerprint
    /// to also catch SDK-side rotation of assume-role/web-identity/IMDS providers.
    /// Returns true when fresh credentials were produced; the caller should then rebuild
    /// the engine and retry. Logs context_for_log on success.
    bool tryRefreshAfterStaleTokenError(
        const DB::Exception & e,
        DB::UInt128 pre_fingerprint,
        const char * context_for_log) const TSA_REQUIRES(mutex);

    std::shared_ptr<KernelSnapshotState> getKernelSnapshotState() const TSA_REQUIRES(mutex);

    /// Starts a snapshot build on a worker thread, reserving one of the bounded worker permits
    /// before the launch, and returns the shared in-flight state for waiters. Never blocks on
    /// the kernel. Static, so that the scan iterator can rebuild through the same path.
    static std::shared_ptr<InflightSnapshotLoad> startKernelSnapshotLoad(
        KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build, const KernelClientOptions & client_options);

    /// Waits for an in-flight build, re-checking the query status on every poll: `KILL QUERY`,
    /// `max_execution_time` and `delta_lake_snapshot_load_timeout_ms` abort the wait for this
    /// waiter only, while the build keeps running for the others. The kernel FFI is synchronous
    /// and has no cancellation hook, so this polling wait is the only cancellation point of a
    /// snapshot load (for example, one stuck on an object store which never answers).
    static void waitForSnapshotLoad(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper);

    /// Marks a load as given up by every waiter (counted in DeltaLakeSnapshotLoadsStuck until
    /// the worker returns). For a shared load this is called under `mutex`, together with the
    /// waiter unregistration, so that `given_up` in initOrUpdateSnapshot never sees a half state.
    static void markAbandoned(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper, const LoggerPtr & log);

    /// Convenience for a single waiter: start, wait (interruptibly) and take the result.
    /// Every `KernelSnapshotState` construction must go through this or the two helpers
    /// above, so that no code path blocks in the kernel without a cancellation point.
    static std::shared_ptr<KernelSnapshotState> loadKernelSnapshotState(
        KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build, const LoggerPtr & log);
};

using TableSnapshotPtr = std::shared_ptr<TableSnapshot>;

}

#endif
