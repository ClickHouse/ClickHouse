#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <IO/S3Common.h>
#include <Disks/DiskCacheWrapper.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/ObjectStorages/S3/ProxyConfiguration.h>
#include <Disks/ObjectStorages/S3/ProxyListConfiguration.h>
#include <Disks/ObjectStorages/S3/ProxyResolverConfiguration.h>
#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Common/FileCacheFactory.h>


namespace DB
{


std::unique_ptr<DiskS3Settings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);

std::shared_ptr<Aws::S3::S3Client> getClient(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);


}
