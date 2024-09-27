#pragma once

#include "config.h"

#if USE_AVRO

#    include <Storages/IStorage.h>
#    include <Storages/ObjectStorage/Azure/Configuration.h>
#    include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#    include <Storages/ObjectStorage/Local/Configuration.h>
#    include <Storages/ObjectStorage/S3/Configuration.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>
#    include <Storages/StorageFactory.h>
#    include <Common/logger_useful.h>

#    include <memory>


namespace DB
{

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorage::Configuration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration
{
public:
    using Configuration = StorageObjectStorage::Configuration;

    bool isDataLakeConfiguration() const override { return true; }

    std::string getEngineName() const override { return DataLakeMetadata::name; }

    static ColumnsDescription getTableStructureFromData(
        [[maybe_unused]] ObjectStoragePtr object_storage_,
        [[maybe_unused]] Configuration * base_configuration,
        [[maybe_unused]] const std::optional<FormatSettings> & format_settings_,
        [[maybe_unused]] ContextPtr local_context)
    {
        return ColumnsDescription{};
    }

    void update(ContextPtr local_context) override
    {
        auto new_metadata = DataLakeMetadata::create(StorageObjectStorage::Configuration::getObjectStorage(), this, local_context);
        if (current_metadata && *current_metadata == *new_metadata)
            return;

        current_metadata = std::move(new_metadata);
        BaseStorageConfiguration::setPaths(current_metadata->getDataFiles());
        BaseStorageConfiguration::setPartitionColumns(current_metadata->getPartitionColumns());
    }

protected:

private:
    DataLakeMetadataPtr current_metadata;

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, supports_subset_of_columns);
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(StorageObjectStorage::Configuration::getObjectStorage(), this, local_context);
        }
        auto column_mapping = current_metadata->getColumnNameToPhysicalNameMapping();
        if (!column_mapping.empty())
        {
            for (const auto & [column_name, physical_name] : column_mapping)
            {
                auto & column = info.format_header.getByName(column_name);
                column.name = physical_name;
            }
        }
        return info;
    }
};

using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;


}

#endif
