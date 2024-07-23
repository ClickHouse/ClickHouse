#include <config.h>

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Requests.h>
#include <Interpreters/Context.h>
#include <Storages/DataLakes/S3MetadataReader.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}

std::shared_ptr<ReadBuffer>
S3DataLakeMetadataReadHelper::createReadBuffer(const String & key, ContextPtr context, const StorageS3::Configuration & base_configuration)
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

bool S3DataLakeMetadataReadHelper::exists(const String & key, const StorageS3::Configuration & configuration)
{
    return S3::objectExists(*configuration.client, configuration.url.bucket, key);
}

std::vector<String> S3DataLakeMetadataReadHelper::listFiles(
    const StorageS3::Configuration & base_configuration, const String & prefix, const String & suffix)
{
    const auto & table_path = base_configuration.url.key;
    const auto & bucket = base_configuration.url.bucket;
    const auto & client = base_configuration.client;

    std::vector<String> res;
    S3::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    request.SetBucket(bucket);
    request.SetPrefix(std::filesystem::path(table_path) / prefix);

    bool is_finished{false};
    while (!is_finished)
    {
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw S3Exception(
                outcome.GetError().GetErrorType(),
                "Could not list objects in bucket {} with key {}, S3 exception: {}, message: {}",
                quoteString(bucket),
                quoteString(base_configuration.url.key),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            const auto & filename = obj.GetKey();
            if (filename.ends_with(suffix))
                res.push_back(filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    LOG_TRACE(getLogger("S3DataLakeMetadataReadHelper"), "Listed {} files", res.size());

    return res;
}

}
#endif
