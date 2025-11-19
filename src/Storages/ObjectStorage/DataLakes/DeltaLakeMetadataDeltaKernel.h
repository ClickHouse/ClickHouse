#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DeltaLake
{
class TableSnapshot;
}

namespace DB
{

class DeltaLakeMetadataDeltaKernel final : public IDataLakeMetadata
{
public:
    static constexpr auto name = "DeltaLake";

    const char * getName() const override { return name; }

    DeltaLakeMetadataDeltaKernel(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        ContextPtr context);

    bool supportsUpdate() const override { return true; }

    bool supportsWrites() const override { return true; }

    void update(const ContextPtr & context) override;

    NamesAndTypesList getTableSchema(ContextPtr local_context) const override;

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns,
        bool supports_tuple_elements) override;

    bool operator ==(const IDataLakeMetadata &) const override;

    void modifyFormatSettings(FormatSettings & format_settings, const Context &) const override;

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationWeakPtr configuration,
        ContextPtr context)
    {
        auto configuration_ptr = configuration.lock();
        return std::make_unique<DeltaLakeMetadataDeltaKernel>(object_storage, configuration, context);
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata_snapshot,

        ContextPtr context) const override;

    DeltaLake::KernelHelperPtr getKernelHelper() const { return kernel_helper; }

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override;

private:
    const LoggerPtr log;
    const DeltaLake::KernelHelperPtr kernel_helper;
    const std::shared_ptr<DeltaLake::TableSnapshot> table_snapshot TSA_GUARDED_BY(table_snapshot_mutex);
    mutable std::mutex table_snapshot_mutex;

    ObjectStoragePtr object_storage_common;
    void logMetadataFiles(ContextPtr context) const;
};

}

#endif
