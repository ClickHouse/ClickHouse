#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DeltaLake
{
class TableSnapshot;
}

namespace DB
{
namespace StorageObjectStorageSetting
{
extern const StorageObjectStorageSettingsBool allow_experimental_delta_kernel_rs;
extern const StorageObjectStorageSettingsBool delta_lake_read_schema_same_as_table_schema;
}

class DeltaLakeMetadataDeltaKernel final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadataDeltaKernel(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        bool read_schema_same_as_table_schema_);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & context) override;

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override;

    NamesAndTypesList getReadSchema() const override;

    bool operator ==(const IDataLakeMetadata &) const override;

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationObserverPtr configuration,
        ContextPtr, bool)
    {
        auto configuration_ptr = configuration.lock();
        const auto & settings_ref = configuration_ptr->getSettingsRef();
        return std::make_unique<DeltaLakeMetadataDeltaKernel>(
            object_storage,
            configuration,
            settings_ref[StorageObjectStorageSetting::delta_lake_read_schema_same_as_table_schema]);
    }

    bool supportsFileIterator() const override { return true; }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size) const override;

private:
    const LoggerPtr log;
    const std::shared_ptr<DeltaLake::TableSnapshot> table_snapshot;
};

}

#endif
