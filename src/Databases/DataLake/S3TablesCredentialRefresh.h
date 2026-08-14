#pragma once

#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/StorageCredentials.h>

#include <aws/core/auth/AWSCredentialsProvider.h>

#include <memory>

namespace DataLake
{

std::shared_ptr<IStorageCredentials> resolveS3TablesRefreshCredentials(
    const ICatalog::CredentialsRefreshCallback & base_callback,
    Aws::Auth::AWSCredentialsProvider & credentials_provider);

}

#endif
