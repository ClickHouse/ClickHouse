#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Formats/FormatFactory.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Storage for read-only integration with Apache Iceberg tables in Amazon S3 (see https://iceberg.apache.org/)
/// Right now it's implemented on top of StorageS3 and right now it doesn't support
/// many Iceberg features like schema evolution, partitioning, positional and equality deletes.
template <typename DataLakeMetadata>
class IStorageDataLake final : public StorageObjectStorage
{
public:
    using Storage = StorageObjectStorage;
    using ConfigurationPtr = Storage::ConfigurationPtr;

    static StoragePtr create(
        ConfigurationPtr base_configuration,
        ContextPtr context,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode)
    {
        auto object_storage = base_configuration->createObjectStorage(context, /* is_readonly */true);
        DataLakeMetadataPtr metadata;
        NamesAndTypesList schema_from_metadata;

        if (base_configuration->format == "auto")
            base_configuration->format = "Parquet";

        ConfigurationPtr configuration = base_configuration->clone();

        try
        {
            metadata = DataLakeMetadata::create(object_storage, base_configuration, context);
            schema_from_metadata = metadata->getTableSchema();
            configuration->setPaths(metadata->getDataFiles());
        }
        catch (...)
        {
            if (mode <= LoadingStrictnessLevel::CREATE)
                throw;

            metadata.reset();
            configuration->setPaths({});
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return std::make_shared<IStorageDataLake<DataLakeMetadata>>(
            base_configuration, std::move(metadata), configuration, object_storage,
            context, table_id_,
            columns_.empty() ? ColumnsDescription(schema_from_metadata) : columns_,
            constraints_, comment_, format_settings_);
    }

    String getName() const override { return DataLakeMetadata::name; }

    static ColumnsDescription getTableStructureFromData(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr base_configuration,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr local_context)
    {
        auto metadata = DataLakeMetadata::create(object_storage_, base_configuration, local_context);

        auto schema_from_metadata = metadata->getTableSchema();
        if (schema_from_metadata != NamesAndTypesList{})
        {
            return ColumnsDescription(std::move(schema_from_metadata));
        }
        else
        {
            ConfigurationPtr configuration = base_configuration->clone();
            configuration->setPaths(metadata->getDataFiles());
            return Storage::resolveSchemaFromData(
                object_storage_, configuration, format_settings_, local_context);
        }
    }

    void updateConfiguration(ContextPtr local_context) override
    {
        Storage::updateConfiguration(local_context);

        auto new_metadata = DataLakeMetadata::create(Storage::object_storage, base_configuration, local_context);

        if (current_metadata && *current_metadata == *new_metadata)
            return;

        current_metadata = std::move(new_metadata);
        auto updated_configuration = base_configuration->clone();
        updated_configuration->setPaths(current_metadata->getDataFiles());

        Storage::configuration = updated_configuration;
    }

    template <typename... Args>
    IStorageDataLake(
        ConfigurationPtr base_configuration_,
        DataLakeMetadataPtr metadata_,
        Args &&... args)
        : Storage(std::forward<Args>(args)...)
        , base_configuration(base_configuration_)
        , current_metadata(std::move(metadata_))
    {
        if (base_configuration->format == "auto")
        {
            base_configuration->format = Storage::configuration->format;
        }
    }

private:
    ConfigurationPtr base_configuration;
    DataLakeMetadataPtr current_metadata;
};

using StorageIceberg = IStorageDataLake<IcebergMetadata>;
using StorageDeltaLake = IStorageDataLake<DeltaLakeMetadata>;
using StorageHudi = IStorageDataLake<HudiMetadata>;

}

#endif
