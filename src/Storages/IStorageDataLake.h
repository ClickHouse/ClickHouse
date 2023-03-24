#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>


namespace DB
{

template <typename Storage, typename Name, typename MetadataParser>
class IStorageDataLake : public Storage
{
public:
    using IConfiguration = typename Storage::Configuration;
    using ConfigurationPtr = std::unique_ptr<IConfiguration>;

    template <class ...Args>
    explicit IStorageDataLake(
        ConfigurationPtr configuration_,
        ContextPtr context_,
        Args && ...args)
        : Storage(createConfigurationForDataRead(context_, *configuration_), context_, std::forward<Args>(args)...)
        , base_configuration(std::move(configuration_))
        , log(&Poco::Logger::get(getName()))
    {
    }

    struct Configuration : public Storage::Configuration
    {
        template <class ...Args>
        explicit Configuration(Args && ...args) : Storage::Configuration(std::forward<Args>(args)...) {}

        bool update(ContextPtr /* context */) override
        {
            return false;
        }
    };

    static constexpr auto name = Name::name;
    String getName() const override { return name; }

    static ConfigurationPtr getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        auto configuration = Storage::getConfiguration(engine_args, local_context, false /* get_format_from_file */);
        /// Data lakes have default format as parquet.
        if (configuration->format == "auto")
            configuration->format = "Parquet";
        return configuration;
    }

    static ColumnsDescription getTableStructureFromData(
        IConfiguration & base_configuration, const std::optional<FormatSettings> & format_settings, ContextPtr local_context)
    {
        auto configuration = createConfigurationForDataRead(local_context, base_configuration);
        return Storage::getTableStructureFromData(*configuration, format_settings, local_context, /*object_infos*/ nullptr);
    }

    // void updateConfiguration(ContextPtr local_context, Configuration & configuration) override
    // {
    //     configuration = createConfigurationForDataRead(local_context, base_configuration);
    // }

private:
    static ConfigurationPtr createConfigurationForDataRead(ContextPtr local_context, const IConfiguration & base_configuration)
    {
        Poco::Logger * log = &Poco::Logger::get("IStorageDataLake");

        auto new_configuration = std::make_unique<Configuration>(base_configuration);
        new_configuration->update(local_context);

        MetadataParser parser{*new_configuration, local_context};
        auto keys = parser.getFiles();
        auto files = MetadataParser::generateQueryFromKeys(keys, new_configuration->format);

        LOG_TEST(log, "FILES: {}, ", fmt::join(files, ", "));
        new_configuration->appendToPath(files);

        LOG_DEBUG(log, "URL for read: {}", new_configuration->getPath());
        return new_configuration;
    }

    ConfigurationPtr base_configuration;
    Poco::Logger * log;
};

}

#endif
