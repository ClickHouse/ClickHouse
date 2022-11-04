#include "config.h"

#if USE_AWS_S3

#    include <Storages/StorageHudi.h>
#    include <Common/logger_useful.h>

#    include <Formats/FormatFactory.h>
#    include <IO/S3Common.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/checkAndGetLiteralArgument.h>
#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/ListObjectsV2Request.h>

#    include <QueryPipeline/Pipe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int S3_ERROR;
}

StorageHudi::StorageHudi(
    const StorageS3Configuration & configuration_,
    const StorageID & table_id_,
    ColumnsDescription columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : IStorage(table_id_)
    , base_configuration{configuration_.url, configuration_.auth_settings, configuration_.rw_settings, configuration_.headers}
    , log(&Poco::Logger::get("StorageHudi (" + table_id_.table_name + ")"))
    , table_path(base_configuration.uri.key)
{
    StorageInMemoryMetadata storage_metadata;
    StorageS3::updateS3Configuration(context_, base_configuration);

    auto keys = getKeysFromS3();

    auto new_uri = base_configuration.uri.uri.toString() + generateQueryFromKeys(std::move(keys), configuration_.format);

    LOG_DEBUG(log, "New uri: {}", new_uri);
    LOG_DEBUG(log, "Table path: {}", table_path);

    StorageS3Configuration new_configuration;
    new_configuration.url = new_uri;
    new_configuration.auth_settings.access_key_id = configuration_.auth_settings.access_key_id;
    new_configuration.auth_settings.secret_access_key = configuration_.auth_settings.secret_access_key;
    new_configuration.format = configuration_.format;

    if (columns_.empty())
    {
        columns_ = StorageS3::getTableStructureFromData(
            new_configuration, /*distributed processing*/ false, format_settings_, context_, nullptr);
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

Pipe StorageHudi::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    StorageS3::updateS3Configuration(context, base_configuration);
    return s3engine->read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

std::vector<std::string> StorageHudi::getKeysFromS3()
{
    std::vector<std::string> keys;

    const auto & client = base_configuration.client;

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};
    const auto bucket{base_configuration.uri.bucket};

    request.SetBucket(bucket);
    request.SetPrefix(table_path);

    while (!is_finished)
    {
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Could not list objects in bucket {} with key {}, S3 exception: {}, message: {}",
                quoteString(bucket),
                quoteString(table_path),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            const auto & filename = obj.GetKey().substr(table_path.size()); // object name without tablepath prefix
            keys.push_back(filename);
            LOG_DEBUG(log, "Found file: {}", filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}

std::string StorageHudi::generateQueryFromKeys(std::vector<std::string> && keys, String format)
{
    // make format lowercase
    std::transform(format.begin(), format.end(), format.begin(), [](unsigned char c) { return std::tolower(c); });

    // filter only files with specific format
    std::erase_if(keys, [&format](const std::string & s) { return std::filesystem::path(s).extension() != "." + format; });

    // for each partition path take only latest file
    std::unordered_map<std::string, std::pair<std::string, uint64_t>> latest_parts;

    for (const auto & key : keys)
    {
        auto slash = key.find_last_of("/");
        std::string path;
        if (slash == std::string::npos)
        {
            path = "";
        }
        else
        {
            path = key.substr(0, slash);
        }

        // every filename contains metadata split by "_", timestamp is after last "_"
        uint64_t timestamp = std::stoul(key.substr(key.find_last_of("_") + 1));

        auto it = latest_parts.find(path);

        if (it != latest_parts.end())
        {
            if (it->second.second < timestamp)
            {
                it->second = {key, timestamp};
            }
        }
        else
        {
            latest_parts[path] = {key, timestamp};
        }
    }

    std::vector<std::string> filtered_keys;
    std::transform(
        latest_parts.begin(), latest_parts.end(), std::back_inserter(filtered_keys), [](const auto & kv) { return kv.second.first; });

    std::string new_query;

    for (auto && key : filtered_keys)
    {
        if (!new_query.empty())
        {
            new_query += ",";
        }
        new_query += key;
    }
    new_query = "{" + new_query + "}";

    return new_query;
}


void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage(
        "Hudi",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            if (engine_args.empty() || engine_args.size() < 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage Hudi requires 3 to 4 arguments: table_url, access_key, secret_access_key, [format]");


            StorageS3Configuration configuration;

            configuration.url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (engine_args.size() == 4)
                configuration.format = checkAndGetLiteralArgument<String>(engine_args[3], "format");

            // Apache Hudi uses Parquet by default
            if (configuration.format == "auto")
                configuration.format = "Parquet";

            auto format_settings = getFormatSettings(args.getContext());

            return std::make_shared<StorageHudi>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), format_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
