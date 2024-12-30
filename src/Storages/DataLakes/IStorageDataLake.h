#pragma once

#include <optional>
#include <variant>
#include "Interpreters/Context.h"
#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>
#include <filesystem>


namespace DB
{

template <typename Storage, typename Name, typename MetadataParser>
class IStorageDataLake : public Storage
{
public:
    static constexpr auto name = Name::name;
    using Configuration = typename Storage::Configuration;

    template <class ...Args>
    explicit IStorageDataLake(const std::optional<Configuration> & configuration_, ContextPtr context_, LoadingStrictnessLevel mode, Args && ...args)
        : Storage(
            configuration_.has_value() ? getConfigurationForDataRead(*configuration_, context_, {}, mode) : std::optional<Configuration>{},
            context_,
            std::forward<Args>(args)...)
        , base_configuration(configuration_)
        , log(getLogger(getName())) {
            if (!base_configuration.has_value()) Storage::namedCollectionDeleted();
        } // NOLINT(clang-analyzer-optin.cplusplus.VirtualCall)

    template <class ...Args>
    static StoragePtr create(const std::optional<Configuration> & configuration_, ContextPtr context_, LoadingStrictnessLevel mode, Args && ...args)
    {
        return std::make_shared<IStorageDataLake<Storage, Name, MetadataParser>>(configuration_, context_, mode, std::forward<Args>(args)...);
    }

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & base_configuration,
        const std::optional<FormatSettings> & format_settings_,
        const ContextPtr & local_context)
    {
        auto configuration = getConfigurationForDataRead(base_configuration, local_context);
        return Storage::getTableStructureFromData(configuration, format_settings_, local_context);
    }

    static std::variant<Configuration, String> getConfiguration(ASTs & engine_args, ContextPtr local_context, bool allow_missing_named_collection = false)
    {
        return Storage::getConfiguration(engine_args, local_context, /* get_format_from_file */false, allow_missing_named_collection);
    }

    Configuration updateConfigurationAndGetCopy(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        Storage::assertNamedCollectionExists();
        updateConfigurationImpl(local_context);
        return Storage::getConfiguration();
    }

    void updateConfiguration(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        if (base_configuration.has_value()) {
            updateConfigurationImpl(local_context);
        }
    }

    void reload(ContextPtr context_, ASTs engine_args) override
    {
        std::lock_guard lock(configuration_update_mutex);
        auto new_configuration = std::get<typename Storage::Configuration>(getConfiguration(engine_args, context_));
        if (new_configuration.format == "auto")
            new_configuration.format = "Parquet";
        base_configuration = new_configuration;
        Storage::namedCollectionRestored();
    }

    String getFormat() const override
    {
        std::lock_guard lock(configuration_update_mutex);
        Storage::assertNamedCollectionExists();
        return base_configuration->format;
    }

private:
    static Configuration getConfigurationForDataRead(
        const Configuration & base_configuration, const ContextPtr & local_context, const Strings & keys = {},
        LoadingStrictnessLevel mode = LoadingStrictnessLevel::CREATE)
    {
        auto configuration{base_configuration};
        configuration.update(local_context);
        configuration.static_configuration = true;

        try
        {
            if (keys.empty())
                configuration.keys = getDataFiles(configuration, local_context);
            else
                configuration.keys = keys;

            LOG_TRACE(
                getLogger("DataLake"),
                "New configuration path: {}, keys: {}",
                configuration.getPath(), fmt::join(configuration.keys, ", "));

            configuration.connect(local_context);
            return configuration;
        }
        catch (...)
        {
            if (mode <= LoadingStrictnessLevel::CREATE)
                throw;
            tryLogCurrentException(__PRETTY_FUNCTION__);
            return configuration;
        }
    }

    static Strings getDataFiles(const Configuration & configuration, const ContextPtr & local_context)
    {
        return MetadataParser().getFiles(configuration, local_context);
    }

    void updateConfigurationImpl(const ContextPtr & local_context)
    {
        const bool updated = base_configuration->update(local_context);
        auto new_keys = getDataFiles(*base_configuration, local_context);

        if (!updated && new_keys == Storage::getConfiguration().keys)
            return;

        Storage::useConfiguration(getConfigurationForDataRead(*base_configuration, local_context, new_keys));
    }


    std::optional<Configuration> base_configuration;
    mutable std::mutex configuration_update_mutex;
    LoggerPtr log;
};


template <typename DataLake>
static StoragePtr createDataLakeStorage(const StorageFactory::Arguments & args)
{
    ContextPtr context = args.getLocalContext();
    auto configuration_or_named_collection_name = DataLake::getConfiguration(args.engine_args, context,
        args.allow_missing_named_collection || context->getSettingsRef().allow_missing_named_collections);

    if (std::holds_alternative<typename  DataLake::Configuration>(configuration_or_named_collection_name)) {
        auto configuration = std::get<typename DataLake::Configuration>(configuration_or_named_collection_name);
        if (configuration.format == "auto")
            configuration.format = "Parquet";
        return DataLake::create(
            configuration, args.getContext(), args.mode, args.table_id, args.columns, args.constraints,
            args.comment, getFormatSettings(args.getContext()),
            configuration.named_collection_name);
    } else {
        return DataLake::create(
            std::optional<typename DataLake::Configuration>{},
            args.getContext(), args.mode, args.table_id, args.columns, args.constraints,
            args.comment, getFormatSettings(args.getContext()),
            std::get<String>(configuration_or_named_collection_name));
    }
}

}

#endif
