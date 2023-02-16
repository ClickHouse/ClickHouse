#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Common/logger_useful.h>

#    include <QueryPipeline/Pipe.h>

#    include <Storages/IStorage.h>
#    include <Storages/StorageS3.h>

#    include <base/JSON.h>

namespace DB
{

template <typename Name, typename MetadataParser>
class IStorageDataLake : public IStorage
{
public:
    // 1. Parses internal file structure of table
    // 2. Finds out parts with latest version
    // 3. Creates url for underlying StorageS3 enigne to handle reads
    IStorageDataLake(
        const StorageS3::Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_)
        : IStorage(table_id_)
        , base_configuration{configuration_}
        , log(&Poco::Logger::get("Storage" + String(name) + "(" + table_id_.table_name + ")"))
    {
        StorageInMemoryMetadata storage_metadata;
        StorageS3::updateS3Configuration(context_, base_configuration);

        auto new_configuration = getAdjustedS3Configuration(context_, base_configuration, log);

        if (columns_.empty())
        {
            columns_ = StorageS3::getTableStructureFromData(new_configuration, format_settings_, context_, nullptr);
            storage_metadata.setColumns(columns_);
        }
        else
            storage_metadata.setColumns(columns_);

        storage_metadata.setConstraints(constraints_);
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);

        s3engine = std::make_shared<StorageS3>(
            new_configuration,
            table_id_,
            columns_,
            constraints_,
            comment,
            context_,
            format_settings_,
            /* distributed_processing_ */ false,
            nullptr);
    }

    static constexpr auto name = Name::name;
    String getName() const override { return name; }

    // Reads latest version of Lake Table
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        StorageS3::updateS3Configuration(context, base_configuration);

        return s3engine->read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    static ColumnsDescription getTableStructureFromData(
        StorageS3::Configuration & configuration, const std::optional<FormatSettings> & format_settings, ContextPtr ctx)
    {
        StorageS3::updateS3Configuration(ctx, configuration);
        auto new_configuration = getAdjustedS3Configuration(ctx, configuration, &Poco::Logger::get("Storage" + String(name)));

        return StorageS3::getTableStructureFromData(new_configuration, format_settings, ctx, /*object_infos*/ nullptr);
    }

    static StorageS3::Configuration
    getAdjustedS3Configuration(const ContextPtr & context, StorageS3::Configuration & configuration, Poco::Logger * log)
    {
        MetadataParser parser{configuration, context};

        auto keys = parser.getFiles();
        String new_uri = std::filesystem::path(configuration.url.uri.toString()) / Name::data_directory_prefix
            / MetadataParser::generateQueryFromKeys(keys, configuration.format);

        StorageS3::Configuration new_configuration(configuration);
        new_configuration.url = S3::URI(new_uri);

        LOG_DEBUG(log, "Table path: {}, new uri: {}", configuration.url.key, new_uri);

        return new_configuration;
    }


    static StorageS3::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context)
    {
        auto configuration = StorageS3::getConfiguration(engine_args, local_context, false /* get_format_from_file */);

        if (configuration.format == "auto")
            configuration.format = "Parquet";

        return configuration;
    }

private:
    StorageS3::Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
};

}

#endif
