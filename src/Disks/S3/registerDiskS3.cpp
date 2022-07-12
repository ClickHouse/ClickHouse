#include <Common/config.h>

#include <base/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include "Disks/DiskFactory.h"

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <IO/S3Common.h>
#include "DiskS3.h"
#include "Disks/DiskCacheWrapper.h"
#include "Storages/StorageS3Settings.h"
#include "ProxyConfiguration.h"
#include "ProxyListConfiguration.h"
#include "ProxyResolverConfiguration.h"
#include "Disks/DiskRestartProxy.h"
#include "Disks/DiskLocal.h"
#include "Disks/RemoteDisksCommon.h"
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

void checkRemoveAccess(IDisk & disk)
{
    disk.removeFile("test_acl");
}

bool checkBatchRemoveIsMissing(DiskS3 & disk, std::unique_ptr<DiskS3Settings> settings, const String & bucket)
{
    const String path = "_test_remove_objects_capability";
    try
    {
        auto file = disk.writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        file->write("test", 4);
        file->finalize();
    }
    catch (...)
    {
        try
        {
            disk.removeFile(path);
        }
        catch (...)
        {
        }
        return false; /// We don't have write access, therefore no information about batch remove.
    }

    /// See `IDiskRemote::removeSharedFile`.
    auto fs_paths_keeper = std::dynamic_pointer_cast<S3PathKeeper>(disk.createFSPathKeeper());
    disk.removeMetadata(path, fs_paths_keeper);

    auto fs_paths_keeper_copy = std::dynamic_pointer_cast<S3PathKeeper>(disk.createFSPathKeeper());
    for (const auto & chunk : fs_paths_keeper->getChunks())
        for (const auto & obj : chunk)
            fs_paths_keeper_copy->addPath(obj.GetKey());

    try
    {
        /// See `DiskS3::removeFromRemoteFS`.
        fs_paths_keeper->removePaths([&](S3PathKeeper::Chunk && chunk)
        {
            String keys = S3PathKeeper::getChunkKeys(chunk);
            LOG_TRACE(&Poco::Logger::get("registerDiskS3"), "Remove AWS keys {}", keys);
            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(chunk);
            Aws::S3::Model::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = settings->client->DeleteObjects(request);
            if (!outcome.IsSuccess())
            {
                const auto & err = outcome.GetError();
                throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
            }
        });
        return false;
    }
    catch (const Exception &)
    {
        fs_paths_keeper_copy->removePaths([&](S3PathKeeper::Chunk && chunk)
        {
            String keys = S3PathKeeper::getChunkKeys(chunk);
            LOG_TRACE(&Poco::Logger::get("registerDiskS3"), "Remove AWS keys {} one by one", keys);
            for (const auto & obj : chunk)
            {
                Aws::S3::Model::DeleteObjectRequest request;
                request.SetBucket(bucket);
                request.SetKey(obj.GetKey());
                settings->client->DeleteObject(request);
            }
        });
        return true;
    }
}

std::shared_ptr<S3::ProxyResolverConfiguration> getProxyResolverConfiguration(
    const String & prefix, const Poco::Util::AbstractConfiguration & proxy_resolver_config)
{
    auto endpoint = Poco::URI(proxy_resolver_config.getString(prefix + ".endpoint"));
    auto proxy_scheme = proxy_resolver_config.getString(prefix + ".proxy_scheme");
    if (proxy_scheme != "http" && proxy_scheme != "https")
        throw Exception("Only HTTP/HTTPS schemas allowed in proxy resolver config: " + proxy_scheme, ErrorCodes::BAD_ARGUMENTS);
    auto proxy_port = proxy_resolver_config.getUInt(prefix + ".proxy_port");
    auto cache_ttl = proxy_resolver_config.getUInt(prefix + ".proxy_cache_time", 10);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Configured proxy resolver: {}, Scheme: {}, Port: {}",
        endpoint.toString(), proxy_scheme, proxy_port);

    return std::make_shared<S3::ProxyResolverConfiguration>(endpoint, proxy_scheme, proxy_port, cache_ttl);
}

std::shared_ptr<S3::ProxyListConfiguration> getProxyListConfiguration(
    const String & prefix, const Poco::Util::AbstractConfiguration & proxy_config)
{
    std::vector<String> keys;
    proxy_config.keys(prefix, keys);

    std::vector<Poco::URI> proxies;
    for (const auto & key : keys)
        if (startsWith(key, "uri"))
        {
            Poco::URI proxy_uri(proxy_config.getString(prefix + "." + key));

            if (proxy_uri.getScheme() != "http" && proxy_uri.getScheme() != "https")
                throw Exception("Only HTTP/HTTPS schemas allowed in proxy uri: " + proxy_uri.toString(), ErrorCodes::BAD_ARGUMENTS);
            if (proxy_uri.getHost().empty())
                throw Exception("Empty host in proxy uri: " + proxy_uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            proxies.push_back(proxy_uri);

            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Configured proxy: {}", proxy_uri.toString());
        }

    if (!proxies.empty())
        return std::make_shared<S3::ProxyListConfiguration>(proxies);

    return nullptr;
}

std::shared_ptr<S3::ProxyConfiguration> getProxyConfiguration(const String & prefix, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(prefix + ".proxy"))
        return nullptr;

    std::vector<String> config_keys;
    config.keys(prefix + ".proxy", config_keys);

    if (auto resolver_configs = std::count(config_keys.begin(), config_keys.end(), "resolver"))
    {
        if (resolver_configs > 1)
            throw Exception("Multiple proxy resolver configurations aren't allowed", ErrorCodes::BAD_ARGUMENTS);

        return getProxyResolverConfiguration(prefix + ".proxy.resolver", config);
    }

    return getProxyListConfiguration(prefix + ".proxy", config);
}

std::shared_ptr<Aws::S3::S3Client>
getClient(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        config.getString(config_prefix + ".region", ""),
        context->getRemoteHostFilter(), context->getGlobalContext()->getSettingsRef().s3_max_redirects);

    S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));
    if (uri.key.back() != '/')
        throw Exception("S3 path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

    client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
    client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
    client_configuration.endpointOverride = uri.endpoint;

    auto proxy_config = getProxyConfiguration(config_prefix, config);
    if (proxy_config)
    {
        client_configuration.perRequestConfiguration
            = [proxy_config](const auto & request) { return proxy_config->getConfiguration(request); };
        client_configuration.error_report
            = [proxy_config](const auto & request_config) { proxy_config->errorReport(request_config); };
    }

    client_configuration.retryStrategy
        = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

    return S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        config.getString(config_prefix + ".access_key_id", ""),
        config.getString(config_prefix + ".secret_access_key", ""),
        config.getString(config_prefix + ".server_side_encryption_customer_key_base64", ""),
        {},
        config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", false)),
        config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false)));
}

std::unique_ptr<DiskS3Settings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    return std::make_unique<DiskS3Settings>(
        getClient(config, config_prefix, context),
        config.getUInt64(config_prefix + ".s3_max_single_read_retries", context->getSettingsRef().s3_max_single_read_retries),
        config.getUInt64(config_prefix + ".s3_min_upload_part_size", context->getSettingsRef().s3_min_upload_part_size),
        config.getUInt64(config_prefix + ".s3_upload_part_size_multiply_factor", context->getSettingsRef().s3_upload_part_size_multiply_factor),
        config.getUInt64(config_prefix + ".s3_upload_part_size_multiply_parts_count_threshold", context->getSettingsRef().s3_upload_part_size_multiply_parts_count_threshold),
        config.getUInt64(config_prefix + ".s3_max_single_part_upload_size", context->getSettingsRef().s3_max_single_part_upload_size),
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getBool(config_prefix + ".send_metadata", false),
        config.getInt(config_prefix + ".thread_pool_size", 16),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000));
}

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
        S3Capabilities s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);

        std::shared_ptr<IDisk> s3disk = std::make_shared<DiskS3>(
            name,
            uri.bucket,
            uri.key,
            metadata_disk,
            std::move(cache),
            context,
            s3_capabilities,
            getSettings(config, config_prefix, context),
            getSettings);

        bool skip_access_check = config.getBool(config_prefix + ".skip_access_check", false);

        if (!skip_access_check)
        {
            /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
            if (s3_capabilities.support_batch_delete && checkBatchRemoveIsMissing(*std::dynamic_pointer_cast<DiskS3>(s3disk), getSettings(config, config_prefix, context), uri.bucket))
            {
                LOG_WARNING(
                    &Poco::Logger::get("registerDiskS3"),
                    "Storage for disk {} does not support batch delete operations, "
                    "so `s3_capabilities.support_batch_delete` was automatically turned off during the access check. "
                    "To remove this message set `s3_capabilities.support_batch_delete` for the disk to `false`.",
                    name
                );
                std::dynamic_pointer_cast<DiskS3>(s3disk)->setCapabilitiesSupportBatchDelete(false);
            }
        }

        /// This code is used only to check access to the corresponding disk.
        if (!skip_access_check)
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
