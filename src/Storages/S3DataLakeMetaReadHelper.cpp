#include <config.h>

#if USE_AWS_S3

#    include <Storages/S3DataLakeMetaReadHelper.h>

namespace DB
{
std::shared_ptr<ReadBuffer>
S3DataLakeMetaReadHelper::createReadBuffer(const String & key, ContextPtr context, const StorageS3::Configuration & base_configuration)
{
    S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
    return std::make_shared<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.url.bucket,
        key,
        base_configuration.url.version_id,
        request_settings,
        context->getReadSettings());
}
std::vector<String> S3DataLakeMetaReadHelper::listFilesMatchSuffix(
    const StorageS3::Configuration & base_configuration, const String & directory, const String & suffix)
{
    const auto & table_path = base_configuration.url.key;
    return S3::listFiles(
        *base_configuration.client, base_configuration.url.bucket, table_path, std::filesystem::path(table_path) / directory, suffix);
}

std::vector<String> S3DataLakeMetaReadHelper::listFiles(const StorageS3::Configuration & configuration)
{
    const auto & client = configuration.client;
    const auto & table_path = configuration.url.key;
    const auto & bucket = configuration.url.bucket;

    std::vector<std::string> keys;
    S3::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};

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
            const auto & filename = obj.GetKey().substr(table_path.size()); /// Object name without tablepath prefix.
            keys.push_back(filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return keys;
}
}
#endif
