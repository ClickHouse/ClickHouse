#include "config.h"
#if USE_AWS_S3

#include <Storages/StorageDeltaLake.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>
#include <IO/S3/Requests.h>

#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Formats/FormatFactory.h>

#include <aws/core/auth/AWSCredentials.h>

#include <QueryPipeline/Pipe.h>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_DATA;
}

void DeltaLakeMetadata::setLastModifiedTime(const String & filename, uint64_t timestamp)
{
    file_update_time[filename] = timestamp;
}

void DeltaLakeMetadata::remove(const String & filename, uint64_t /*timestamp */)
{
    bool erase = file_update_time.erase(filename);
    if (!erase)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid table metadata, tried to remove {} before adding it", filename);
}

std::vector<String> DeltaLakeMetadata::listCurrentFiles() &&
{
    std::vector<String> keys;
    keys.reserve(file_update_time.size());

    for (auto && [k, _] : file_update_time)
        keys.push_back(k);

    return keys;
}

JsonMetadataGetter::JsonMetadataGetter(const StorageS3::Configuration & configuration_, ContextPtr context)
    : base_configuration(configuration_)
{
    init(context);
}

void JsonMetadataGetter::init(ContextPtr context)
{
    auto keys = getJsonLogFiles();

    // read data from every json log file
    for (const String & key : keys)
    {
        auto buf = createS3ReadBuffer(key, context);

        char c;
        while (!buf->eof())
        {
            /// May be some invalid characters before json.
            while (buf->peek(c) && c != '{')
                buf->ignore();

            if (buf->eof())
                break;

            String json_str;
            readJSONObjectPossiblyInvalid(json_str, *buf);

            if (json_str.empty())
                continue;

            const JSON json(json_str);
            handleJSON(json);
        }
    }
}

std::vector<String> JsonMetadataGetter::getJsonLogFiles() const
{
    const auto & client = base_configuration.client;
    const auto table_path = base_configuration.url.key;
    const auto bucket = base_configuration.url.bucket;

    std::vector<String> keys;
    S3::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};

    request.SetBucket(bucket);

    /// DeltaLake format stores all metadata json files in _delta_log directory
    static constexpr auto deltalake_metadata_directory = "_delta_log";
    request.SetPrefix(std::filesystem::path(table_path) / deltalake_metadata_directory);

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
            const auto & filename = obj.GetKey();

            // DeltaLake metadata files have json extension
            if (std::filesystem::path(filename).extension() == ".json")
                keys.push_back(filename);
        }

        /// Needed in case any more results are available
        /// if so, we will continue reading, and not read keys that were already read
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

        /// Set to false if all of the results were returned. Set to true if more keys
        /// are available to return. If the number of results exceeds that specified by
        /// MaxKeys, all of the results might not be returned
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}

std::shared_ptr<ReadBuffer> JsonMetadataGetter::createS3ReadBuffer(const String & key, ContextPtr context)
{
    /// TODO: add parallel downloads
    S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = 10;
    return std::make_shared<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.url.bucket,
        key,
        base_configuration.url.version_id,
        request_settings,
        context->getReadSettings());
}

void JsonMetadataGetter::handleJSON(const JSON & json)
{
    if (json.has("add"))
    {
        auto path = json["add"]["path"].getString();
        auto timestamp = json["add"]["modificationTime"].getInt();

        metadata.setLastModifiedTime(path, timestamp);
    }
    else if (json.has("remove"))
    {
        auto path = json["remove"]["path"].getString();
        auto timestamp = json["remove"]["deletionTimestamp"].getInt();

        metadata.remove(path, timestamp);
    }
}

namespace
{

// DeltaLake stores data in parts in different files
// keys is vector of parts with latest version
// generateQueryFromKeys constructs query from parts filenames for
// underlying StorageS3 engine
String generateQueryFromKeys(const std::vector<String> & keys)
{
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}


StorageS3::Configuration getAdjustedS3Configuration(
    const ContextPtr & context, const StorageS3::Configuration & configuration, Poco::Logger * log)
{
    JsonMetadataGetter getter{configuration, context};
    const auto keys = getter.getFiles();
    const auto new_uri = configuration.url.uri.toString() + generateQueryFromKeys(keys);

    // set new url in configuration
    StorageS3::Configuration new_configuration(configuration);
    new_configuration.url = S3::URI(new_uri);

    LOG_DEBUG(log, "Table path: {}, new uri: {}", configuration.url.key, new_uri);
    return new_configuration;
}

}

StorageDeltaLake::StorageDeltaLake(
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    ColumnsDescription columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : IStorage(table_id_)
    , base_configuration{configuration_}
    , log(&Poco::Logger::get("StorageDeltaLake (" + table_id_.table_name + ")"))
    , table_path(base_configuration.url.key)
{
    StorageInMemoryMetadata storage_metadata;
    StorageS3::updateS3Configuration(context_, base_configuration);
    auto new_configuration = getAdjustedS3Configuration(context_, base_configuration, log);

    if (columns_.empty())
    {
        columns_ = StorageS3::getTableStructureFromData(
            new_configuration, format_settings_, context_, nullptr);
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

Pipe StorageDeltaLake::read(
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

ColumnsDescription StorageDeltaLake::getTableStructureFromData(
    StorageS3::Configuration & configuration, const std::optional<FormatSettings> & format_settings, ContextPtr ctx)
{
    StorageS3::updateS3Configuration(ctx, configuration);
    auto new_configuration = getAdjustedS3Configuration(ctx, configuration, &Poco::Logger::get("StorageDeltaLake"));
    return StorageS3::getTableStructureFromData(new_configuration, format_settings, ctx, /*object_infos*/ nullptr);
}

void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            if (engine_args.empty() || engine_args.size() < 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage DeltaLake requires 3 to 4 arguments: table_url, access_key, secret_access_key, [format]");

            StorageS3::Configuration configuration;

            configuration.url = S3::URI(checkAndGetLiteralArgument<String>(engine_args[0], "url"));
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (engine_args.size() == 4)
                configuration.format = checkAndGetLiteralArgument<String>(engine_args[3], "format");
            else
            {
                /// DeltaLake uses Parquet by default.
                configuration.format = "Parquet";
            }

            return std::make_shared<StorageDeltaLake>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), std::nullopt);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
