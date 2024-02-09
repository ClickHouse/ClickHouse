#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>


namespace DB
{

template <typename StorageSettings, typename Name, typename MetadataParser>
class IStorageDataLake : public StorageObjectStorage<StorageSettings>
{
public:
    static constexpr auto name = Name::name;

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
        bool /* attach */)
    {
        auto object_storage = base_configuration->createOrUpdateObjectStorage(context);

        auto configuration = base_configuration->clone();
        configuration->getPaths() = MetadataParser().getFiles(object_storage, configuration, context);

        return std::make_shared<IStorageDataLake<StorageSettings, Name, MetadataParser>>(
            base_configuration, configuration, object_storage, engine_name_, context,
            table_id_, columns_, constraints_, comment_, format_settings_);
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

        auto new_keys = MetadataParser().getFiles(Storage::object_storage, base_configuration, local_context);

        if (updated || new_keys != Storage::configuration->getPaths())
        {
            auto updated_configuration = base_configuration->clone();
            /// If metadata wasn't changed, we won't list data files again.
            updated_configuration->getPaths() = new_keys;
            Storage::configuration = updated_configuration;
        }
        return {Storage::configuration, Storage::object_storage};
    }

    template <typename... Args>
    explicit IStorageDataLake(
        ConfigurationPtr base_configuration_,
        Args &&... args)
        : Storage(std::forward<Args>(args)...)
        , base_configuration(base_configuration_)
    {
    }

private:
    ConfigurationPtr base_configuration;
    LoggerPtr log;
};


}

#endif
