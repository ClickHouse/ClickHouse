#include <Common/config.h>

#if USE_AWS_S3

#include <Storages/StorageDelta.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>

#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <QueryPipeline/Pipe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
}

void DeltaLakeMetadata::add(const String & key, uint64_t timestamp)
{
    file_update_time[key] = timestamp;
}

void DeltaLakeMetadata::remove(const String & key, uint64_t /*timestamp */)
{
    file_update_time.erase(key);
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

JsonMetadataGetter::JsonMetadataGetter(StorageS3::S3Configuration & configuration_, const String & table_path_, Poco::Logger * log_)
    : base_configuration(configuration_), table_path(table_path_), metadata(), log(log_)
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
        String json_str;
        size_t opening(0), closing(0);
        char c;

        while (buf->read(c))
        {
            // skip all space characters for JSON to parse correctly
            if (isspace(c))
            {
                continue;
            }

            json_str.push_back(c);

            if (c == '{')
                opening++;
            else if (c == '}')
                closing++;

            if (opening == closing)
            {
                LOG_DEBUG(log, "JSON {}, {}", json_str, json_str.size());

                JSON json(json_str);

                if (json.has("add"))
                {
                    auto path = json["add"]["path"].getString();
                    auto timestamp = json["add"]["modificationTime"].getInt();

                    metadata.add(path, timestamp);

                    LOG_DEBUG(log, "Path {}", path);
                    LOG_DEBUG(log, "Timestamp {}", timestamp);
                }
                else if (json.has("remove"))
                {
                    auto path = json["remove"]["path"].getString();
                    auto timestamp = json["remove"]["deletionTimestamp"].getInt();

                    metadata.remove(path, timestamp);

                    LOG_DEBUG(log, "Path {}", path);
                    LOG_DEBUG(log, "Timestamp {}", timestamp);
                }

                // reset
                opening = 0;
                closing = 0;
                json_str.clear();
            }
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
    request.SetPrefix(table_path + "_delta_log");

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

            if (filename.substr(filename.size() - 5) == ".json")
                keys.push_back(filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}

std::unique_ptr<ReadBuffer> JsonMetadataGetter::createS3ReadBuffer(const String & key)
{

    // TBD: add parallel downloads
    return std::make_unique<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.uri.bucket,
        key,
        base_configuration.uri.version_id,
        /* max single read retries */ 10,
        ReadSettings{});
}

StorageDelta::StorageDelta(
    const S3::URI & uri_,
    const String & access_key_,
    const String & secret_access_key_,
    const StorageID & table_id_,
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
    updateS3Configuration(context_, base_configuration);

    JsonMetadataGetter getter{base_configuration, table_path, log};

    auto keys = getter.getFiles();

    for (const String & path : keys)
    {
        LOG_DEBUG(log, "{}", path);
    }

    auto new_uri = base_configuration.uri.uri.toString() + generateQueryFromKeys(std::move(keys));

    LOG_DEBUG(log, "New uri: {}", new_uri);
    LOG_DEBUG(log, "Table path: {}", table_path);
    auto s3_uri = S3::URI(Poco::URI(new_uri));

    if (columns_.empty())
    {
        columns_
            = StorageS3::getTableStructureFromData(String("Parquet"), s3_uri, access_key_, secret_access_key_, "", false, {}, context_);
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
        String("Parquet"), // format name
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
    updateS3Configuration(context, base_configuration);

    return s3engine->read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

void StorageDelta::updateS3Configuration(ContextPtr ctx, StorageS3::S3Configuration & upd)
{
    auto settings = ctx->getStorageS3Settings().getSettings(upd.uri.uri.toString());

    bool need_update_configuration = settings != S3Settings{};
    if (need_update_configuration)
    {
        if (upd.rw_settings != settings.rw_settings)
            upd.rw_settings = settings.rw_settings;
    }

    upd.rw_settings.updateFromSettingsIfEmpty(ctx->getSettings());

    if (upd.client && (!upd.access_key_id.empty() || settings.auth_settings == upd.auth_settings))
        return;

    Aws::Auth::AWSCredentials credentials(upd.access_key_id, upd.secret_access_key);
    HeaderCollection headers;
    if (upd.access_key_id.empty())
    {
        credentials = Aws::Auth::AWSCredentials(settings.auth_settings.access_key_id, settings.auth_settings.secret_access_key);
        headers = settings.auth_settings.headers;
    }

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        settings.auth_settings.region,
        ctx->getRemoteHostFilter(),
        ctx->getGlobalContext()->getSettingsRef().s3_max_redirects,
        ctx->getGlobalContext()->getSettingsRef().enable_s3_requests_logging);

    client_configuration.endpointOverride = upd.uri.endpoint;
    client_configuration.maxConnections = upd.rw_settings.max_connections;

    upd.client = S3::ClientFactory::instance().create(
        client_configuration,
        upd.uri.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        settings.auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        settings.auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
        settings.auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));

    upd.auth_settings = std::move(settings.auth_settings);
}

String StorageDelta::generateQueryFromKeys(std::vector<String> && keys)
{
    String new_query;

    for (auto && key : keys)
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


void registerStorageDelta(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            if (engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

            auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());

            configuration.url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");


            S3::URI s3_uri(Poco::URI(configuration.url));

            return std::make_shared<StorageDelta>(
                s3_uri,
                configuration.auth_settings.access_key_id,
                configuration.auth_settings.secret_access_key,
                args.table_id,
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
