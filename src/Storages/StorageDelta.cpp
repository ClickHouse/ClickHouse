#include "config.h"
#if USE_AWS_S3

#include <Storages/StorageDelta.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>

#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Formats/FormatFactory.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

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

std::vector<String> DeltaLakeMetadata::ListCurrentFiles() &&
{
    std::vector<String> keys;
    keys.reserve(file_update_time.size());

    for (auto && [k, _] : file_update_time)
        keys.push_back(k);

    return keys;
}

JsonMetadataGetter::JsonMetadataGetter(StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context)
    : base_configuration(configuration_), table_path(table_path_)
{
    Init(context);
}

void JsonMetadataGetter::Init(ContextPtr context)
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

std::vector<String> JsonMetadataGetter::getJsonLogFiles()
{
    std::vector<String> keys;

    const auto & client = base_configuration.client;

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};
    const auto bucket{base_configuration.uri.bucket};

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
        base_configuration.uri.bucket,
        key,
        base_configuration.uri.version_id,
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

StorageDelta::StorageDelta(
    const StorageS3Configuration & configuration_,
    const StorageID & table_id_,
    ColumnsDescription columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : IStorage(table_id_)
    , base_configuration{configuration_.url, configuration_.auth_settings, configuration_.request_settings, configuration_.headers}
    , log(&Poco::Logger::get("StorageDeltaLake (" + table_id_.table_name + ")"))
    , table_path(base_configuration.uri.key)
{
    StorageInMemoryMetadata storage_metadata;
    StorageS3::updateS3Configuration(context_, base_configuration);

    JsonMetadataGetter getter{base_configuration, table_path, context_};

    auto keys = getter.getFiles();
    auto new_uri = base_configuration.uri.uri.toString() + generateQueryFromKeys(std::move(keys));

    LOG_DEBUG(log, "New uri: {}", new_uri);
    LOG_DEBUG(log, "Table path: {}", table_path);

    // set new url in configuration
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

Pipe StorageDelta::read(
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

String StorageDelta::generateQueryFromKeys(std::vector<String> && keys)
{
    // DeltaLake store data parts in different files
    // keys are filenames of parts
    // for StorageS3 to read all parts we need format {key1,key2,key3,...keyn}
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}

void registerStorageDelta(StorageFactory & factory)
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

            StorageS3Configuration configuration;

            configuration.url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (engine_args.size() == 4)
                configuration.format = checkAndGetLiteralArgument<String>(engine_args[3], "format");
            else
            {
                /// DeltaLake uses Parquet by default.
                configuration.format = "Parquet";
            }

            return std::make_shared<StorageDelta>(
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
