#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
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
    explicit IStorageDataLake(const Configuration & configuration_, ContextPtr context_, Args && ...args)
        : Storage(getConfigurationForDataRead(configuration_, context_), context_, std::forward<Args>(args)...)
        , base_configuration(configuration_)
        , log(&Poco::Logger::get(getName())) {}

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & base_configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr local_context)
    {
        auto configuration = getConfigurationForDataRead(base_configuration, local_context);
        return Storage::getTableStructureFromData(configuration, format_settings, local_context);
    }

    static Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        return Storage::getConfiguration(engine_args, local_context, /* get_format_from_file */false);
    }

    Configuration updateConfigurationAndGetCopy(ContextPtr local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
        return Storage::getConfiguration();
    }

    void updateConfiguration(ContextPtr local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
    }

private:
    static Configuration getConfigurationForDataRead(
        const Configuration & base_configuration, ContextPtr local_context, const Strings & keys = {})
    {
        auto configuration{base_configuration};
        configuration.update(local_context);
        configuration.static_configuration = true;

        if (keys.empty())
            configuration.keys = getDataFiles(configuration, local_context);
        else
            configuration.keys = keys;

        LOG_TRACE(
            &Poco::Logger::get("DataLake"),
            "New configuration path: {}, keys: {}",
            configuration.getPath(), fmt::join(configuration.keys, ", "));

        configuration.connect(local_context);
        return configuration;
    }

    static Strings getDataFiles(const Configuration & configuration, ContextPtr local_context)
    {
        return MetadataParser().getFiles(configuration, local_context);
    }

    void updateConfigurationImpl(ContextPtr local_context)
    {
        const bool updated = base_configuration.update(local_context);
        auto new_keys = getDataFiles(base_configuration, local_context);

        if (!updated && new_keys == Storage::getConfiguration().keys)
            return;

        Storage::useConfiguration(getConfigurationForDataRead(base_configuration, local_context, new_keys));
    }

    Configuration base_configuration;
    std::mutex configuration_update_mutex;
    Poco::Logger * log;
};


template <typename DataLake>
static StoragePtr createDataLakeStorage(const StorageFactory::Arguments & args)
{
    auto configuration = DataLake::getConfiguration(args.engine_args, args.getLocalContext());

    /// Data lakes use parquet format, no need for schema inference.
    if (configuration.format == "auto")
        configuration.format = "Parquet";

    return std::make_shared<DataLake>(
        configuration, args.getContext(), args.table_id, args.columns, args.constraints,
        args.comment, getFormatSettings(args.getContext()));
}

}

#endif
