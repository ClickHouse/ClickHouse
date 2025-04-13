#pragma once

#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/StorageFactory.h>
#include <Common/logger_useful.h>
#include "Storages/ColumnsDescription.h"

#include <memory>
#include <string>

#include <Common/ErrorCodes.h>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
extern const int FORMAT_VERSION_TOO_OLD;
}

namespace StorageObjectStorageSetting
{
extern const StorageObjectStorageSettingsBool allow_dynamic_metadata_for_data_lakes;
}


template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorage::Configuration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorage::Configuration>
{
public:
    using Configuration = StorageObjectStorage::Configuration;

    bool isDataLakeConfiguration() const override { return true; }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    void update(ObjectStoragePtr object_storage, ContextPtr local_context) override
    {
        BaseStorageConfiguration::update(object_storage, local_context);

        bool existed = current_metadata != nullptr;

        if (updateMetadataObjectIfNeeded(object_storage, local_context))
        {
            if (hasExternalDynamicMetadata() && existed)
            {
                throw Exception(
                    ErrorCodes::FORMAT_VERSION_TOO_OLD,
                    "Metadata is not consinsent with the one which was used to infer table schema. Please, retry the query.");
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

    std::optional<size_t> totalRows() override
    {
        if (!current_metadata)
            return {};

        return current_metadata->totalRows();
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
        return BaseStorageConfiguration::getSettingsRef()[StorageObjectStorageSetting::allow_dynamic_metadata_for_data_lakes]
            && current_metadata
            && current_metadata->supportsSchemaEvolution();
    }

    ColumnsDescription updateAndGetCurrentSchema(
        ObjectStoragePtr object_storage,
        ContextPtr context) override
    {
        BaseStorageConfiguration::update(object_storage, context);
        updateMetadataObjectIfNeeded(object_storage, context);
        return ColumnsDescription{current_metadata->getTableSchema()};
    }

    bool supportsFileIterator() const override { return true; }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size) override
    {
        chassert(current_metadata);
        return current_metadata->iterate(filter_dag, callback, list_batch_size);
    }

    /// This is an awful temporary crutch,
    /// which will be removed once DeltaKernel is used by default for DeltaLake.
    /// By release 25.3.
    /// (Because it does not make sense to support it in a nice way
    /// because the code will be removed ASAP anyway)
#if USE_PARQUET && USE_AWS_S3
    DeltaLakePartitionColumns getDeltaLakePartitionColumns() const
    {
        const auto * delta_lake_metadata = dynamic_cast<const DeltaLakeMetadata *>(current_metadata.get());
        if (delta_lake_metadata)
            return delta_lake_metadata->getPartitionColumns();
        return {};
    }
#endif

private:
    DataLakeMetadataPtr current_metadata;
    LoggerPtr log = getLogger("DataLakeConfiguration");

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(
                object_storage,
                weak_from_this(),
                local_context);
        }
        return current_metadata->prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
    }

    bool updateMetadataObjectIfNeeded(
        ObjectStoragePtr object_storage,
        ContextPtr context)
    {
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(
                object_storage,
                weak_from_this(),
                context);
            return true;
        }

        if (current_metadata->supportsUpdate())
        {
            return current_metadata->update(context);
        }

        auto new_metadata = DataLakeMetadata::create(
            object_storage,
            weak_from_this(),
            context);

        if (*current_metadata != *new_metadata)
        {
            current_metadata = std::move(new_metadata);
            return true;
        }
        else
        {
            return false;
        }
    }
};


#if USE_AVRO
#    if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
#endif

#if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
#endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
#endif

#if USE_PARQUET
#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureDeltaLakeConfiguration = DataLakeConfiguration<StorageAzureConfiguration, DeltaLakeMetadata>;
#endif

using StorageLocalDeltaLakeConfiguration = DataLakeConfiguration<StorageLocalConfiguration, DeltaLakeMetadata>;

#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
