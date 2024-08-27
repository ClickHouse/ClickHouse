#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

#include <vector>

namespace DB
{

class DeltaLakeMetadata final : public IDataLakeMetadata
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;
    static constexpr auto name = "DeltaLake";

    DeltaLakeMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        ContextPtr context_);

    std::vector<DataFileInfo> getDataFilesInfo() const override { return data_files; }

    NamesAndTypesList getTableSchema() const override { return schema; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * deltalake_metadata = dynamic_cast<const DeltaLakeMetadata *>(&other);
        return deltalake_metadata
            && !data_files.empty() && !deltalake_metadata->data_files.empty()
            && data_files == deltalake_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        ContextPtr local_context)
    {
        return std::make_unique<DeltaLakeMetadata>(object_storage, configuration, local_context);
    }

private:
    mutable std::vector<DataFileInfo> data_files;
    NamesAndTypesList schema;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
};

}
