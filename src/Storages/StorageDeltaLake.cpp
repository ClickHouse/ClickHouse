#include "config.h"
#if USE_AWS_S3

#include <Storages/StorageDeltaLake.h>
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

std::vector<String> DeltaLakeMetadata::listCurrentFiles() &&
{
    std::vector<String> keys;
    keys.reserve(file_update_time.size());

    for (auto && [k, _] : file_update_time)
        keys.push_back(k);

    return keys;
}

DeltaLakeMetaParser::DeltaLakeMetaParser(StorageS3::S3Configuration & configuration_, const String & table_path_, ContextPtr context)
    : base_configuration(configuration_), table_path(table_path_)
{
    init(context);
}

void DeltaLakeMetaParser::init(ContextPtr context)
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

std::vector<String> DeltaLakeMetaParser::getJsonLogFiles()
{
    /// DeltaLake format stores all metadata json files in _delta_log directory
    static constexpr auto deltalake_metadata_directory = "_delta_log";

    return S3::listFiles(
        *base_configuration.client,
        base_configuration.uri.bucket,
        table_path,
        std::filesystem::path(table_path) / deltalake_metadata_directory,
        ".json");
}

std::shared_ptr<ReadBuffer> DeltaLakeMetaParser::createS3ReadBuffer(const String & key, ContextPtr context)
{
    S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
    return std::make_shared<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.uri.bucket,
        key,
        base_configuration.uri.version_id,
        request_settings,
        context->getReadSettings());
}

void DeltaLakeMetaParser::handleJSON(const JSON & json)
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

// DeltaLake stores data in parts in different files
// keys is vector of parts with latest version
// generateQueryFromKeys constructs query from parts filenames for
// underlying StorageS3 engine
String DeltaLakeMetaParser::generateQueryFromKeys(const std::vector<String> & keys, const String &)
{
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}

void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [](const StorageFactory::Arguments & args)
        {
            StorageS3Configuration configuration = StorageDeltaLake::getConfiguration(args.engine_args, args.getLocalContext());

            auto format_settings = getFormatSettings(args.getContext());

            return std::make_shared<StorageDeltaLake>(
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
