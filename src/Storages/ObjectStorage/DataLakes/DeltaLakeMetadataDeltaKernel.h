#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Common/CacheBase.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeTableStateSnapshot.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace DeltaLake
{
class TableSnapshot;
using TableSnapshotPtr = std::shared_ptr<TableSnapshot>;
class TableChanges;
using TableChangesPtr = std::shared_ptr<TableChanges>;
using TableChangesVersionRange = std::pair<size_t, std::optional<size_t>>;
}

namespace DB
{

class DeltaLakeMetadataDeltaKernel final : public IDataLakeMetadata
{
public:
    static constexpr auto name = "DeltaLake";
    using SnapshotVersion = UInt64;

    const char * getName() const override { return name; }

    DeltaLakeMetadataDeltaKernel(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_);

    bool supportsUpdate() const override { return true; }

    bool supportsWrites() const override { return true; }

    void update(const ContextPtr & context) override;

    NamesAndTypesList getTableSchema(ContextPtr local_context) const override;

    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr) const override;
    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(const DataLakeTableStateSnapshot &, ContextPtr) const override;
    bool shouldReloadSchemaForConsistency(ContextPtr) const override;

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements) override;

    bool operator ==(const IDataLakeMetadata &) const override;

    void modifyFormatSettings(FormatSettings & format_settings, const Context &) const override;

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration)
    {
        auto configuration_ptr = configuration.lock();
        return std::make_unique<DeltaLakeMetadataDeltaKernel>(object_storage_, configuration);
    }

    std::optional<size_t> totalRows(ContextPtr) const override;

    std::optional<size_t> totalBytes(ContextPtr) const override;

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata_snapshot,

        ContextPtr context) const override;

    DeltaLake::KernelHelperPtr getKernelHelper() const { return kernel_helper; }

    DeltaLake::TableChangesPtr getTableChanges(
        const DeltaLake::TableChangesVersionRange & version_range,
        const Block & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context) const;

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override;

private:
    using TableSnapshotCache = CacheBase<SnapshotVersion, DeltaLake::TableSnapshot>;

    const LoggerPtr log;
    const DeltaLake::KernelHelperPtr kernel_helper;
    const ObjectStoragePtr object_storage;
    const std::string format_name;

    mutable TableSnapshotCache snapshots TSA_GUARDED_BY(snapshots_mutex);
    mutable std::mutex snapshots_mutex;
    mutable std::optional<SnapshotVersion> latest_snapshot_version;

    void logMetadataFiles(ContextPtr context) const;

    /// No version means latest version.
    DeltaLake::TableSnapshotPtr getTableSnapshot(
        std::optional<SnapshotVersion> version = std::nullopt) const;

    std::string latestSnapshotVersionToStr() const TSA_REQUIRES(snapshots_mutex);
};

}

#endif
