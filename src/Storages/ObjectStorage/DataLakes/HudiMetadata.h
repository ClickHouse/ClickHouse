#pragma once

#include <Interpreters/Context_fwd.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Core/Types.h>

namespace DB
{

class HudiMetadata final : public IDataLakeMetadata, private WithContext
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto name = "Hudi";

    HudiMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        ContextPtr context_);

    Strings getDataFiles() const override;

    NamesAndTypesList getTableSchema() const override { return {}; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * hudi_metadata = dynamic_cast<const HudiMetadata *>(&other);
        return hudi_metadata
            && !data_files.empty() && !hudi_metadata->data_files.empty()
            && data_files == hudi_metadata->data_files;
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        ContextPtr local_context)
    {
        return std::make_unique<HudiMetadata>(object_storage, configuration, local_context);
    }

private:
    const ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    mutable Strings data_files;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;

    Strings getDataFilesImpl() const;
};

}
