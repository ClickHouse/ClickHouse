#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/S3TablesCredentialRefresh.h>

namespace DataLake
{

namespace
{

std::shared_ptr<IStorageCredentials> getCatalogIAMCredentials(Aws::Auth::AWSCredentialsProvider & provider)
{
    auto aws_creds = provider.GetAWSCredentials();
    if (aws_creds.GetAWSAccessKeyId().empty() || aws_creds.GetAWSSecretKey().empty())
        return nullptr;
    return std::make_shared<S3Credentials>(
        aws_creds.GetAWSAccessKeyId(), aws_creds.GetAWSSecretKey(), aws_creds.GetSessionToken());
}

}

std::shared_ptr<IStorageCredentials> resolveS3TablesRefreshCredentials(
    const ICatalog::CredentialsRefreshCallback & base_callback,
    Aws::Auth::AWSCredentialsProvider & credentials_provider)
{
    if (base_callback)
    {
        if (auto creds = (*base_callback)())
        {
            auto s3_creds = std::dynamic_pointer_cast<S3Credentials>(creds);
            if (s3_creds && !s3_creds->isEmpty())
                return creds;
        }
    }

    return getCatalogIAMCredentials(credentials_provider);
}

}

#endif
