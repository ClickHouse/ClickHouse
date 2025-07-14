#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
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

    DeltaLakeMetadataDeltaKernel(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & context) override;

    NamesAndTypesList getTableSchema() const override;

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns) override;

    bool operator ==(const IDataLakeMetadata &) const override;

    void modifyFormatSettings(FormatSettings & format_settings) const override;

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationWeakPtr configuration,
        ContextPtr /* context */)
    {
        auto configuration_ptr = configuration.lock();
        return std::make_unique<DeltaLakeMetadataDeltaKernel>(object_storage, configuration);
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        ContextPtr context) const override;

private:
    const LoggerPtr log;
    const std::shared_ptr<DeltaLake::TableSnapshot> table_snapshot;
};

}

#endif
