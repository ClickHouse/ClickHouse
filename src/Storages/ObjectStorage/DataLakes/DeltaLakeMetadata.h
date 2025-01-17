#pragma once

#include "config.h"

#if USE_PARQUET

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

class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_);

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override { return schema; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    bool operator ==(const IDataLakeMetadata &) const override
    {
        return false;
        // const auto * deltalake_metadata = dynamic_cast<const DeltaLakeMetadata *>(&other);
        // return deltalake_metadata
        //     && !data_files.empty() && !deltalake_metadata->data_files.empty()
        //     && data_files == deltalake_metadata->data_files;
    }

    static DataLakeMetadataPtr create(ObjectStoragePtr object_storage, ConfigurationObserverPtr configuration, ContextPtr local_context)
    {
        return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
    }

    bool supportsFileIterator() const override { return true; }

    ObjectIterator iterate() const override;

private:
    std::shared_ptr<DeltaLake::TableSnapshot> table_snapshot;
    NamesAndTypesList schema;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
    LoggerPtr log;
};

}

#endif
