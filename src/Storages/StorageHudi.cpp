#include <Storages/StorageHudi.h>
#include <Common/config.h>
#include <Common/logger_useful.h>

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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int S3_ERROR;
}

StorageHudi::StorageHudi(
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
    , log(&Poco::Logger::get("StorageHudi (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    updateS3Configuration(context_, base_configuration);

    auto keys = getKeysFromS3();

    auto new_uri = base_configuration.uri.uri.toString() + generateQueryFromKeys(std::move(keys));

    LOG_DEBUG(log, "New uri: {}", new_uri);

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

Pipe StorageHudi::read(
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

void StorageHudi::updateS3Configuration(ContextPtr ctx, StorageS3::S3Configuration & upd)
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

std::vector<std::string> StorageHudi::getKeysFromS3()
{
    std::vector<std::string> keys;

    const auto & client = base_configuration.client;

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};
    const auto bucket{base_configuration.uri.bucket};
    const std::string key = "";

    request.SetBucket(bucket);

    while (!is_finished)
    {
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Could not list objects in bucket {} with key {}, S3 exception: {}, message: {}",
                quoteString(bucket),
                quoteString(key),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            const auto & filename = obj.GetKey();
            keys.push_back(filename);
            //LOG_DEBUG(log, "Found file: {}", filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}

std::string StorageHudi::generateQueryFromKeys(std::vector<std::string> && keys)
{
    // filter only .parquet files
    std::erase_if(
        keys,
        [](const std::string & s)
        {
            if (s.size() >= 8)
            {
                return s.substr(s.size() - 8) != ".parquet";
            }
            return true;
        });

    // for each partition path take only latest parquet file

    std::unordered_map<std::string, std::pair<std::string, uint64_t>> latest_parquets;

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

        uint64_t timestamp = std::stoul(key.substr(key.find_last_of("_") + 1));

        auto it = latest_parquets.find(path);

        if (it != latest_parquets.end())
        {
            if (it->second.second < timestamp)
            {
                it->second = {key, timestamp};
            }
        }
        else
        {
            latest_parquets[path] = {key, timestamp};
        }
    }

    std::vector<std::string> filtered_keys;
    std::transform(
        latest_parquets.begin(), latest_parquets.end(), std::back_inserter(filtered_keys), [](const auto & kv) { return kv.second.first; });

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
            if (engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

            auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());

            configuration.url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");


            S3::URI s3_uri(Poco::URI(configuration.url));

            return std::make_shared<StorageHudi>(
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
