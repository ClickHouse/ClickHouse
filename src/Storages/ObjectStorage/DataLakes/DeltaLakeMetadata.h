#pragma once

#include "config.h"

#if USE_PARQUET

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{
struct DataLakePartitionColumn
{
    NameAndTypePair name_and_type;
    Field value;

    bool operator ==(const DataLakePartitionColumn & other) const = default;
};

/// Data file -> partition columns
using DataLakePartitionColumns = std::unordered_map<std::string, std::vector<DataLakePartitionColumn>>;


class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    Strings getDataFiles() const override { return data_files; }

    NamesAndTypesList getTableSchema() const override { return schema; }

    // const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

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
        bool)
    {
        return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
    }

private:
    mutable Strings data_files;
    NamesAndTypesList schema;
    DataLakePartitionColumns partition_columns;
};

}

#endif
