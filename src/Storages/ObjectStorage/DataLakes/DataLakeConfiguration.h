#pragma once

#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/logger_useful.h>
#include "Storages/ColumnsDescription.h"

#include <memory>
#include <string>
#include <unordered_map>

#include <Common/ErrorCodes.h>


namespace DB
{

namespace ErrorCodes
{
extern const int FORMAT_VERSION_TOO_OLD;
}

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorage::Configuration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorage::Configuration>
{
public:
    using Configuration = StorageObjectStorage::Configuration;

    bool isDataLakeConfiguration() const override { return true; }

    std::string getEngineName() const override { return DataLakeMetadata::name; }

    void update(ObjectStoragePtr object_storage, ContextPtr local_context) override
    {
        BaseStorageConfiguration::update(object_storage, local_context);
        auto new_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), local_context);

        if (!current_metadata || (*current_metadata != *new_metadata))
        {
            if (hasExternalDynamicMetadata())
            {
                throw Exception(
                    ErrorCodes::FORMAT_VERSION_TOO_OLD,
                    "Metadata is not consinsent with the one which was used to infer table schema. Please, retry the query.");
            }
            else
            {
                current_metadata = std::move(new_metadata);
                BaseStorageConfiguration::setPaths(current_metadata->getDataFiles());
                BaseStorageConfiguration::setPartitionColumns(current_metadata->getPartitionColumns());
            }
        }
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const override
    {
        if (!current_metadata)
            return std::nullopt;
        auto schema_from_metadata = current_metadata->getTableSchema();
        if (!schema_from_metadata.empty())
        {
            return ColumnsDescription(std::move(schema_from_metadata));
        }
        return std::nullopt;
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        if (!current_metadata)
            return {};
        return current_metadata->getInitialSchemaByPath(data_path);
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        if (!current_metadata)
            return {};
        return current_metadata->getSchemaTransformer(data_path);
    }

    bool hasExternalDynamicMetadata() override
    {
        return StorageObjectStorage::Configuration::allow_dynamic_metadata_for_data_lakes && current_metadata
            && current_metadata->supportsExternalMetadataChange();
    }

    ColumnsDescription updateAndGetCurrentSchema(ObjectStoragePtr object_storage, ContextPtr context) override
    {
        BaseStorageConfiguration::update(object_storage, context);
        auto new_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), context);

        if (!current_metadata || (*current_metadata != *new_metadata))
        {
            current_metadata = std::move(new_metadata);
            BaseStorageConfiguration::setPaths(current_metadata->getDataFiles());
            BaseStorageConfiguration::setPartitionColumns(current_metadata->getPartitionColumns());
        }
        return ColumnsDescription{current_metadata->getTableSchema()};
    }


private:
    DataLakeMetadataPtr current_metadata;

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), local_context);
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

#if USE_AVRO
#if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
#    endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
#    endif

#if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
#    endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
#endif

#if USE_PARQUET
#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#    endif
#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
