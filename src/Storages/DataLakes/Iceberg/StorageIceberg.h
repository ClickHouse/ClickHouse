#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#    include <filesystem>
#    include <Formats/FormatFactory.h>
#    include <Storages/DataLakes/Iceberg/IcebergMetadata.h>
#    include <Storages/IStorage.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/StorageS3.h>
#    include <Common/logger_useful.h>


namespace DB
{

/// Storage for read-only integration with Apache Iceberg tables in Amazon S3 (see https://iceberg.apache.org/)
/// Right now it's implemented on top of StorageS3 and right now it doesn't support
/// many Iceberg features like schema evolution, partitioning, positional and equality deletes.
/// TODO: Implement Iceberg as a separate storage using IObjectStorage
/// (to support all object storages, not only S3) and add support for missing Iceberg features.
class StorageIceberg : public StorageS3
{
public:
    static constexpr auto name = "Iceberg";

    using Configuration = StorageS3::Configuration;

    static StoragePtr create(const Configuration & base_configuration,
        ContextPtr context_,
        LoadingStrictnessLevel mode,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_);

    StorageIceberg(
        std::unique_ptr<IcebergMetadata> metadata_,
        const Configuration & configuration_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_);

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & base_configuration,
        const std::optional<FormatSettings> &,
        const ContextPtr & local_context);

    static Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        return StorageS3::getConfiguration(engine_args, local_context, /* get_format_from_file */false);
    }

    Configuration updateConfigurationAndGetCopy(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
        return StorageS3::getConfiguration();
    }

    void updateConfiguration(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
    }

private:
    void updateConfigurationImpl(const ContextPtr & local_context);

    std::unique_ptr<IcebergMetadata> current_metadata;
    Configuration base_configuration;
    std::mutex configuration_update_mutex;
};

}

#endif
