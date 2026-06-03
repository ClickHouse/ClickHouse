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
        KernelSnapshotState(const IKernelHelper & helper_, std::optional<size_t> snapshot_version_);

        KernelExternEngine engine;
        KernelSnapshot snapshot;
        KernelScan scan;
        size_t snapshot_version;
    };
    mutable std::shared_ptr<KernelSnapshotState> kernel_snapshot_state;

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

    void initOrUpdateSnapshot() const TSA_REQUIRES(mutex);
    void initOrUpdateSchemaIfChanged() const TSA_REQUIRES(mutex);

    SnapshotStats getSnapshotStats() const TSA_REQUIRES(mutex);
    SnapshotStats getSnapshotStatsImpl() const TSA_REQUIRES(mutex);

    std::shared_ptr<KernelSnapshotState> getKernelSnapshotState() const TSA_REQUIRES(mutex);
};

using TableSnapshotPtr = std::shared_ptr<TableSnapshot>;

}

#endif
