#include <Common/config.h>

#if USE_AWS_S3

#    include <Storages/StorageDelta.h>
#    include <Common/logger_useful.h>

#    include <IO/ReadBufferFromS3.h>
#    include <IO/ReadHelpers.h>
#    include <IO/ReadSettings.h>
#    include <IO/S3Common.h>

#    include <Storages/StorageFactory.h>
#    include <Storages/checkAndGetLiteralArgument.h>

#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/ListObjectsV2Request.h>

#    include <QueryPipeline/Pipe.h>

#    include <fmt/format.h>
#    include <fmt/ranges.h>

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
    {
        keys.push_back(k);
    }

    return keys;
}

JsonMetadataGetter::JsonMetadataGetter(StorageS3::S3Configuration & configuration_, const String & table_path_)
    : base_configuration(configuration_), table_path(table_path_)
{
    Init();
}

void JsonMetadataGetter::Init()
{
    auto keys = getJsonLogFiles();

    // read data from every json log file
    for (const String & key : keys)
    {
        auto buf = createS3ReadBuffer(key);

        while (!buf->eof())
        {
            // may be some invalid characters before json
            char c;
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
    request.SetPrefix(std::filesystem::path(table_path) / "_delta_log");

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

            if (std::filesystem::path(filename).extension() == ".json")
                keys.push_back(filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}

std::shared_ptr<ReadBuffer> JsonMetadataGetter::createS3ReadBuffer(const String & key)
{
    // TBD: add parallel downloads
    return std::make_shared<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.uri.bucket,
        key,
        base_configuration.uri.version_id,
        /* max single read retries */ 10,
        ReadSettings{});
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
    const S3::URI & uri_,
    const String & access_key_,
    const String & secret_access_key_,
    const StorageID & table_id_,
    const String & format_name_,
    ColumnsDescription columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_)
    : IStorage(table_id_)
    , base_configuration({uri_, access_key_, secret_access_key_, {}, {}, {}})
    , log(&Poco::Logger::get("StorageDeltaLake (" + table_id_.table_name + ")"))
    , table_path(uri_.key)
{
    StorageInMemoryMetadata storage_metadata;
    StorageS3::updateS3Configuration(context_, base_configuration);

    JsonMetadataGetter getter{base_configuration, table_path};

    auto keys = getter.getFiles();

    auto new_uri = base_configuration.uri.uri.toString() + generateQueryFromKeys(std::move(keys));

    LOG_DEBUG(log, "New uri: {}", new_uri);
    LOG_DEBUG(log, "Table path: {}", table_path);
    auto s3_uri = S3::URI(Poco::URI(new_uri));

    if (columns_.empty())
    {
        columns_ = StorageS3::getTableStructureFromData(format_name_, s3_uri, access_key_, secret_access_key_, "", false, {}, context_);
        storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    s3engine = std::make_shared<StorageS3>(
        s3_uri,
        access_key_,
        secret_access_key_,
        table_id_,
        format_name_,
        base_configuration.rw_settings,
        columns_,
        constraints_,
        comment,
        context_,
        std::nullopt);
}

Pipe StorageDelta::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    StorageS3::updateS3Configuration(context, base_configuration);

    return s3engine->read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

String StorageDelta::generateQueryFromKeys(std::vector<String> && keys)
{
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


            String table_url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            String access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            String secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            String format = "Parquet";
            if (engine_args.size() == 4)
            {
                format = checkAndGetLiteralArgument<String>(engine_args[3], "format");
            }

            auto s3_uri = S3::URI(Poco::URI(table_url));

            return std::make_shared<StorageDelta>(
                s3_uri,
                access_key_id,
                secret_access_key,
                args.table_id,
                format,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
