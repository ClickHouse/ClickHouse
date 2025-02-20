#pragma once

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataOld.h>
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
}

class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & context) override;

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override;

    NamesAndTypesList getReadSchema() const override;

    bool operator ==(const IDataLakeMetadata &) const override;

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationObserverPtr configuration,
        ContextPtr local_context,
        bool allow_experimental_delta_kernel_rs)
    {
        if (allow_experimental_delta_kernel_rs)
            return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
        else
            return std::make_unique<DeltaLakeMetadataOld>(object_storage, configuration, local_context);
    }

    bool supportsFileIterator() const override { return true; }

    ObjectIterator iterate() const override;

private:
    const LoggerPtr log;
    const std::shared_ptr<DeltaLake::TableSnapshot> table_snapshot;
};

}

#endif
