#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Formats/FormatFactory.h>
#include <Storages/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>


namespace DB
{

/// Storage for read-only integration with Apache Iceberg tables in Amazon S3 (see https://iceberg.apache.org/)
/// Right now it's implemented on top of StorageS3 and right now it doesn't support
/// many Iceberg features like schema evolution, partitioning, positional and equality deletes.
/// TODO: Implement Iceberg as a separate storage using IObjectStorage
/// (to support all object storages, not only S3) and add support for missing Iceberg features.
template <typename StorageSettings>
class StorageIceberg : public StorageObjectStorage<StorageSettings>
{
public:
    static constexpr auto name = "Iceberg";
    using Storage = StorageObjectStorage<StorageSettings>;
    using ConfigurationPtr = Storage::ConfigurationPtr;

    static StoragePtr create(
        ConfigurationPtr base_configuration,
        ContextPtr context,
        const String & engine_name_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::optional<FormatSettings> format_settings_,
        bool attach)
    {
        auto object_storage = base_configuration->createOrUpdateObjectStorage(context);
        std::unique_ptr<IcebergMetadata> metadata;
        NamesAndTypesList schema_from_metadata;
        try
        {
            metadata = parseIcebergMetadata(object_storage, base_configuration, context);
            schema_from_metadata = metadata->getTableSchema();
        }
        catch (...)
        {
            if (!attach)
                throw;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        auto configuration = base_configuration->clone();
        configuration->getPaths() = metadata->getDataFiles();

        return std::make_shared<StorageIceberg<StorageSettings>>(
            base_configuration, std::move(metadata), configuration, object_storage, engine_name_, context,
            table_id_,
            columns_.empty() ? ColumnsDescription(schema_from_metadata) : columns_,
            constraints_, comment_, format_settings_);
    }

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr base_configuration,
        const std::optional<FormatSettings> &,
        ContextPtr local_context)
    {
        auto metadata = parseIcebergMetadata(object_storage_, base_configuration, local_context);
        return ColumnsDescription(metadata->getTableSchema());
    }

    std::pair<ConfigurationPtr, ObjectStoragePtr> updateConfigurationAndGetCopy(ContextPtr local_context) override
    {
        std::lock_guard lock(Storage::configuration_update_mutex);

        auto new_object_storage = base_configuration->createOrUpdateObjectStorage(local_context);
        bool updated = new_object_storage != nullptr;
        if (updated)
            Storage::object_storage = new_object_storage;

        auto new_metadata = parseIcebergMetadata(Storage::object_storage, base_configuration, local_context);

        if (!current_metadata || new_metadata->getVersion() != current_metadata->getVersion())
            current_metadata = std::move(new_metadata);
        else if (updated)
        {
            auto updated_configuration = base_configuration->clone();
            /// If metadata wasn't changed, we won't list data files again.
            updated_configuration->getPaths() = current_metadata->getDataFiles();
            Storage::configuration = updated_configuration;
        }
        return {Storage::configuration, Storage::object_storage};
    }

    template <typename... Args>
    StorageIceberg(
        ConfigurationPtr base_configuration_,
        std::unique_ptr<IcebergMetadata> metadata_,
        Args &&... args)
        : Storage(std::forward<Args>(args)...)
        , base_configuration(base_configuration_)
        , current_metadata(std::move(metadata_))
    {
    }

private:
    ConfigurationPtr base_configuration;
    std::unique_ptr<IcebergMetadata> current_metadata;
};
}

#endif
