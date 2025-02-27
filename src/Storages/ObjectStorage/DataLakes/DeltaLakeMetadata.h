#pragma once

#include "config.h"

#if USE_PARQUET

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{
namespace StorageObjectStorageSetting
{
extern const StorageObjectStorageSettingsBool allow_experimental_delta_kernel_rs;
}

struct DeltaLakePartitionColumn
{
    NameAndTypePair name_and_type;
    Field value;

    bool operator ==(const DeltaLakePartitionColumn & other) const = default;
};

/// Data file -> partition columns
using DeltaLakePartitionColumns = std::unordered_map<std::string, std::vector<DeltaLakePartitionColumn>>;


class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    Strings getDataFiles() const override { return data_files; }

    NamesAndTypesList getTableSchema() const override { return schema; }

    DeltaLakePartitionColumns getPartitionColumns() const { return partition_columns; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * deltalake_metadata = dynamic_cast<const DeltaLakeMetadata *>(&other);
        return deltalake_metadata
            && !data_files.empty() && !deltalake_metadata->data_files.empty()
            && data_files == deltalake_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationObserverPtr configuration,
        ContextPtr local_context,
        [[maybe_unused]] bool allow_experimental_delta_kernel_rs)
    {
#if USE_DELTA_KERNEL_RS
        if (allow_experimental_delta_kernel_rs)
            return std::make_unique<DeltaLakeMetadataDeltaKernel>(object_storage, configuration, local_context);
        else
            return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
#else
        return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
#endif
    }

private:
    mutable Strings data_files;
    NamesAndTypesList schema;
    DeltaLakePartitionColumns partition_columns;
};

}

#endif
