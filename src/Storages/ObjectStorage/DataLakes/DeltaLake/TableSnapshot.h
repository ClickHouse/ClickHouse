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
#include <Poco/JSON/Array.h>
#include <unordered_set>
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

    /// Nothing is installed and every waiter gave up on the only load.
    bool isAbandonedWithoutWaiters() const;

    bool isInitialized() const;

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
    /// Raw Delta `StructType.fields` JSON of this snapshot, preserving exact Delta types (`binary`,
    /// `timestamp_ntz`, ...) that `getTableSchema` collapses. Used for catalog registration.
    Poco::JSON::Array::Ptr getRawDeltaSchemaFields() const;
    /// Get read schema derived from data files.
    /// (In most cases it would be the same as table schema).
    const DB::NamesAndTypesList & getReadSchema() const;
    /// DeltaLake stores partition columns values not in the data files,
    /// but in data file path directory names.
    /// Therefore "table schema" would contain partition columns,
    /// but "read schema" would not.
    const DB::Names & getPartitionColumns() const;
    const DB::NameToNameMap & getPhysicalNamesMap() const;
    const std::unordered_set<String> & getTimestampNtzPaths() const;

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
        /// `used_credentials_fingerprint` is set before the kernel calls, so it is known even if the build fails.
        KernelSnapshotState(
            const IKernelHelper & helper_,
            std::optional<size_t> snapshot_version_,
            DB::UInt128 & used_credentials_fingerprint);

        KernelExternEngine engine;
        KernelSnapshot snapshot;
        KernelScan scan;
        size_t snapshot_version;
        DB::UInt128 credentials_fingerprint{};
    };
    mutable std::shared_ptr<KernelSnapshotState> kernel_snapshot_state;
    mutable DB::UInt128 kernel_state_credentials_fingerprint{};
    /// Forces a rebuild after a credentials refresh even if the fingerprint did not change.
    mutable bool kernel_state_needs_rebuild = false;

    /// A kernel snapshot build running on a worker thread. Waiters poll it outside `mutex`,
    /// each with its own cancellation checks.
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
        /// Registered under `TableSnapshot::mutex`; a load is dead only when nobody waits for it.
        std::atomic<Int64> waiters{0};
        /// Written by the worker before the future is ready; the stale-token retry compares against it.
        DB::UInt128 credentials_fingerprint{};
    };
    mutable std::shared_ptr<InflightSnapshotLoad> inflight_load TSA_GUARDED_BY(mutex);

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
        std::unordered_set<String> timestamp_ntz_paths;
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

    /// Must be called without `mutex`: it takes the lock itself and releases it while waiting.
    void initOrUpdateSnapshot() const TSA_NO_THREAD_SAFETY_ANALYSIS;
    void initOrUpdateSchemaIfChanged() const TSA_REQUIRES(mutex);

    /// Takes `mutex` itself, so that a stale-credentials rebuild is not done under the lock.
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

    /// Starts a build on a worker thread within the bounded worker permits; never blocks on the kernel.
    static std::shared_ptr<InflightSnapshotLoad> startKernelSnapshotLoad(
        KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build);

    /// The only cancellation point of a snapshot load: `KILL QUERY`, `max_execution_time` and
    /// `delta_lake_snapshot_load_timeout_ms` abort this waiter only; the build keeps running.
    static void waitForSnapshotLoad(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper);

    /// For a shared load, called under `mutex` together with the waiter unregistration.
    static void markAbandoned(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper, const LoggerPtr & log);

    /// Every `KernelSnapshotState` is built through this or the helpers above, so no path blocks
    /// in the kernel without a cancellation point.
    static std::shared_ptr<KernelSnapshotState> loadKernelSnapshotState(
        KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build, const LoggerPtr & log);
};

using TableSnapshotPtr = std::shared_ptr<TableSnapshot>;

}

#endif
