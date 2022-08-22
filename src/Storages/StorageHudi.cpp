
#include <Common/logger_useful.h>

#include <Storages/StorageHudi.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>


namespace DB {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int S3_ERROR;
}

StorageHudi::StorageHudi(
    const S3::URI & uri_,
    const String & access_key_,
    const String & secret_access_key,
    const StorageID & table_id_,
    ColumnsDescription /*columns_description_*/,
    ConstraintsDescription /*constraints_description_*/,
    const String & /*comment*/,
    ContextPtr context_
) : IStorage(table_id_)
    , s3_configuration({uri_, access_key_, secret_access_key, {}, {}, {}})
    , log(&Poco::Logger::get("StorageHudi (" + table_id_.table_name + ")"))
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri_.uri);
    updateS3Configuration(context_, s3_configuration);

    const auto & client = s3_configuration.client;
    {
        Aws::S3::Model::ListObjectsV2Request request;
        Aws::S3::Model::ListObjectsV2Outcome outcome;

        const auto key = "hudi";
        bool is_finished{false};
        const auto bucket{s3_configuration.uri.bucket};

        request.SetBucket(bucket);
        //request.SetPrefix(key);

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
                const auto& filename = obj.GetKey();
                LOG_DEBUG(log, "Found file: {}", filename);
            }

            request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
            is_finished = !outcome.GetResult().GetIsTruncated();
        }
    }

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
        ctx->getRemoteHostFilter(), ctx->getGlobalContext()->getSettingsRef().s3_max_redirects,
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



void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage("Hudi", [](const StorageFactory::Arguments & args)
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
        });
}

}

