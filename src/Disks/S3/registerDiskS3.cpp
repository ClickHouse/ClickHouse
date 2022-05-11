#include <Common/config.h>

#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include "Disks/DiskFactory.h"

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <IO/S3Common.h>
#include <Disks/DiskObjectStorage.h>
#include <Disks/DiskCacheWrapper.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/S3/ProxyConfiguration.h>
#include <Disks/S3/ProxyListConfiguration.h>
#include <Disks/S3/ProxyResolverConfiguration.h>
#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskLocal.h>
#include <Disks/RemoteDisksCommon.h>
#include <Disks/S3/diskSettings.h>
#include <Common/FileCacheFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

void checkWriteAccess(IDisk & disk)
{
    auto file = disk.writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    file->write("test", 4);
}

void checkReadAccess(const String & disk_name, IDisk & disk)
{
    auto file = disk.readFile("test_acl");
    String buf(4, '0');
    file->readStrict(buf.data(), 4);
    if (buf != "test")
        throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
}

void checkRemoveAccess(IDisk & disk) { disk.removeFile("test_acl"); }

}

void registerDiskS3(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr {
        S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));

        if (uri.key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No key in S3 uri: {}", uri.uri.toString());

        if (uri.key.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 path must ends with '/', but '{}' doesn't.", uri.key);

        auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);

        FileCachePtr cache = getCachePtrForDisk(name, config, config_prefix, context);

        bool send_metadata = config.getBool(config_prefix + ".send_metadata", false);

        ObjectStoragePtr s3_storage = std::make_unique<S3ObjectStorage>(
            std::move(cache), getClient(config, config_prefix, context),
            getSettings(config, config_prefix, context),
            uri.version_id, uri.bucket);

        std::shared_ptr<IDisk> s3disk = std::make_shared<DiskObjectStorage>(
            name,
            uri.key,
            "DiskS3",
            metadata_disk,
            std::move(s3_storage),
            DiskType::S3,
            send_metadata);

        /// This code is used only to check access to the corresponding disk.
        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*s3disk);
            checkReadAccess(name, *s3disk);
            checkRemoveAccess(*s3disk);
        }

        s3disk->startup();


#ifdef NDEBUG
        bool use_cache = true;
#else
        /// Current S3 cache implementation lead to allocations in destructor of
        /// read buffer.
        bool use_cache = false;
#endif

        if (config.getBool(config_prefix + ".cache_enabled", use_cache))
        {
            String cache_path = config.getString(config_prefix + ".cache_path", context->getPath() + "disks/" + name + "/cache/");
            s3disk = wrapWithCache(s3disk, "s3-cache", cache_path, metadata_path);
        }

        return std::make_shared<DiskRestartProxy>(s3disk);
    };
    factory.registerDiskType("s3", creator);
}

}

#else

void registerDiskS3(DiskFactory &) {}

#endif
