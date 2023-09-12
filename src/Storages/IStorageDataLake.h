#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Common/logger_useful.h>
#    include <Storages/IStorage.h>

#    include <filesystem>

#    include <fmt/format.h>

#    include <IO/S3/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename Storage, typename Name, typename MetadataParser>
class IStorageDataLake : public Storage
{
public:
    using Configuration = typename Storage::Configuration;
    // 1. Parses internal file structure of table
    // 2. Finds out parts with latest version
    // 3. Creates url for underlying StorageS3 enigne to handle reads
    IStorageDataLake(
        const Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_)
        : Storage(
            getAdjustedConfiguration(
                context_, Storage::updateConfiguration(context_, configuration_), &Poco::Logger::get("Storage" + String(name))),
            table_id_,
            columns_,
            constraints_,
            comment,
            context_,
            format_settings_)
    {
    }

    static constexpr auto name = Name::name;
    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & configuration, const std::optional<FormatSettings> & format_settings, ContextPtr ctx)
    {
        Storage::updateConfiguration(ctx, configuration);

        auto new_configuration = getAdjustedConfiguration(ctx, configuration, &Poco::Logger::get("Storage" + String(name)));

        return Storage::getTableStructureFromData(new_configuration, format_settings, ctx, /*object_infos*/ nullptr);
    }

    static Configuration
    getAdjustedConfiguration(const ContextPtr & context, const Configuration & configuration, Poco::Logger * log)
    {
        MetadataParser parser{configuration, context};

        auto keys = parser.getFiles();

        Configuration new_configuration(configuration);

        new_configuration.appendToPath(
            std::filesystem::path(Name::data_directory_prefix) / MetadataParser::generateQueryFromKeys(keys, configuration.format));

        LOG_DEBUG(log, "Table path: {}, new uri: {}", configuration.url.key, configuration.getPath());

        return new_configuration;
    }

    static Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        auto configuration = Storage::getConfiguration(engine_args, local_context, false /* get_format_from_file */);

        if (configuration.format == "auto")
            configuration.format = "Parquet";

        return configuration;
    }

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not supported by storage {}", getName());
    }

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*local_context*/,
        TableExclusiveLockHolder &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported by storage {}", getName());
    }

    NamesAndTypesList getVirtuals() const override { return {}; }
};

}

#endif
