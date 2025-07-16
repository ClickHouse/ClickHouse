#pragma once

#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>
#include <Formats/FormatParserGroup.h>

#include <memory>
#include <string>

#include <Common/ErrorCodes.h>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int LOGICAL_ERROR;
}

namespace DataLakeStorageSetting
{
    extern DataLakeStorageSettingsBool allow_dynamic_metadata_for_data_lakes;
}


template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorageConfiguration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorageConfiguration>
{
public:
    explicit DataLakeConfiguration(DataLakeStorageSettingsPtr settings_) : settings(settings_) {}

    bool isDataLakeConfiguration() const override { return true; }

    const DataLakeStorageSettings & getDataLakeSettings() const override { return *settings; }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    /// Returns true, if metadata is of the latest version, false if unknown.
    bool update(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        bool if_not_updated_before,
        bool check_consistent_with_previous_metadata) override
    {
        const bool updated_before = current_metadata != nullptr;
        if (updated_before && if_not_updated_before)
            return false;

        BaseStorageConfiguration::update(
            object_storage, local_context, if_not_updated_before, check_consistent_with_previous_metadata);

        const bool changed = updateMetadataIfChanged(object_storage, local_context);
        if (!changed)
            return true;

        if (check_consistent_with_previous_metadata && hasExternalDynamicMetadata() && updated_before)
        {
            throw Exception(
                ErrorCodes::FORMAT_VERSION_TOO_OLD,
                "Metadata is not consinsent with the one which was used to infer table schema. "
                "Please, retry the query.");
        }
        return true;
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const override
    {
        assertInitialized();
        if (auto schema = current_metadata->getTableSchema(); !schema.empty())
            return ColumnsDescription(std::move(schema));
        return std::nullopt;
    }

    std::optional<size_t> totalRows(ContextPtr local_context) override
    {
        assertInitialized();
        return current_metadata->totalRows(local_context);
    }

    std::optional<size_t> totalBytes(ContextPtr local_context) override
    {
        assertInitialized();
        return current_metadata->totalBytes(local_context);
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, const String & data_path) const override
    {
        assertInitialized();
        return current_metadata->getInitialSchemaByPath(local_context, data_path);
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, const String & data_path) const override
    {
        assertInitialized();
        return current_metadata->getSchemaTransformer(local_context, data_path);
    }

    bool hasExternalDynamicMetadata() override
    {
        assertInitialized();
        return (*settings)[DataLakeStorageSetting::allow_dynamic_metadata_for_data_lakes]
            && current_metadata->supportsSchemaEvolution();
    }

    IDataLakeMetadata * getExternalMetadata() override
    {
        assertInitialized();
        return current_metadata.get();
    }

    bool supportsFileIterator() const override { return true; }

    bool supportsWrites() const override
    {
        assertInitialized();
        return current_metadata->supportsWrites();
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        ContextPtr context) override
    {
        assertInitialized();
        return current_metadata->iterate(filter_dag, callback, list_batch_size, context);
    }

    /// This is an awful temporary crutch,
    /// which will be removed once DeltaKernel is used by default for DeltaLake.
    /// By release 25.3.
    /// (Because it does not make sense to support it in a nice way
    /// because the code will be removed ASAP anyway)
#if USE_PARQUET && USE_AWS_S3
    DeltaLakePartitionColumns getDeltaLakePartitionColumns() const
    {
        assertInitialized();
        const auto * delta_lake_metadata = dynamic_cast<const DeltaLakeMetadata *>(current_metadata.get());
        if (delta_lake_metadata)
            return delta_lake_metadata->getPartitionColumns();
        return {};
    }
#endif

    void modifyFormatSettings(FormatSettings & settings_) const override
    {
        assertInitialized();
        current_metadata->modifyFormatSettings(settings_);
    }

    ColumnMapperPtr getColumnMapper() const override
    {
        return current_metadata->getColumnMapper();
    }

private:
    DataLakeMetadataPtr current_metadata;
    LoggerPtr log = getLogger("DataLakeConfiguration");
    const DataLakeStorageSettingsPtr settings;

    void assertInitialized() const
    {
        if (!current_metadata)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata is not initialized");
    }

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
        return current_metadata->prepareReadingFromFormat(
            requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
    }

    bool updateMetadataIfChanged(
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

        if (*current_metadata == *new_metadata)
            return false;

        current_metadata = std::move(new_metadata);
        return true;
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
