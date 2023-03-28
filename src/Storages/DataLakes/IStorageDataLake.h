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

    void updateConfigurationIfChanged(ContextPtr local_context) override
    {
        const bool updated = base_configuration.update(local_context);
        auto new_keys = getDataFiles(base_configuration, local_context);

        if (!updated && new_keys == Storage::getConfiguration().keys)
            return;

        Storage::useConfiguration(getConfigurationForDataRead(base_configuration, local_context, new_keys));
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

        LOG_TRACE(&Poco::Logger::get("DataLake"), "New configuration path: {}", configuration.getPath());

        configuration.connect(local_context);
        return configuration;
    }

    static Strings getDataFiles(const Configuration & configuration, ContextPtr local_context)
    {
        auto files =  MetadataParser::getFiles(configuration, local_context);
        for (auto & file : files)
            file = std::filesystem::path(configuration.getPath()) / file;
        return files;
    }

    Configuration base_configuration;
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
