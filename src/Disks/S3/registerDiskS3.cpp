#include <Common/config.h>

#include <base/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include "Disks/DiskFactory.h"

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <IO/S3Common.h>
#include "DiskS3.h"
#include "Disks/DiskCacheWrapper.h"
#include "Storages/StorageS3Settings.h"
#include "ProxyConfiguration.h"
#include "ProxyListConfiguration.h"
#include "ProxyResolverConfiguration.h"
#include "Disks/DiskRestartProxy.h"


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
        if (uri.key.back() != '/')
            throw Exception("S3 path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

        String metadata_path = config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
        fs::create_directories(metadata_path);

        std::shared_ptr<IDisk> s3disk = std::make_shared<DiskS3>(
            name,
            uri.bucket,
            uri.key,
            metadata_path,
            context,
            getSettings(config, config_prefix, context),
            getSettings);

        /// This code is used only to check access to the corresponding disk.
        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*s3disk);
            checkReadAccess(name, *s3disk);
            checkRemoveAccess(*s3disk);
        }

        s3disk->startup();

        bool cache_enabled = config.getBool(config_prefix + ".cache_enabled", true);

        if (cache_enabled)
        {
            String cache_path = config.getString(config_prefix + ".cache_path", context->getPath() + "disks/" + name + "/cache/");

            if (metadata_path == cache_path)
                throw Exception("Metadata and cache path should be different: " + metadata_path, ErrorCodes::BAD_ARGUMENTS);

            auto cache_disk = std::make_shared<DiskLocal>("s3-cache", cache_path, 0);
            auto cache_file_predicate = [] (const String & path)
            {
                return path.ends_with("idx") // index files.
                       || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") // mark files.
                       || path.ends_with("txt") || path.ends_with("dat");
            };

            s3disk = std::make_shared<DiskCacheWrapper>(s3disk, cache_disk, cache_file_predicate);
        }

        return std::make_shared<DiskRestartProxy>(s3disk);
    };
    factory.registerDiskType("s3", creator);
}

}

#else

void registerDiskS3(DiskFactory &) {}

#endif
