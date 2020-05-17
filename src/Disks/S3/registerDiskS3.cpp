#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include "DiskS3.h"
#include "Disks/DiskFactory.h"
#include "ProxyConfiguration.h"
#include "ProxyListConfiguration.h"
#include "ProxyResolverConfiguration.h"

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
        auto file = disk.readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
        String buf(4, '0');
        file->readStrict(buf.data(), 4);
        if (buf != "test")
            throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
    }

    void checkRemoveAccess(IDisk & disk) { disk.remove("test_acl"); }

    std::shared_ptr<S3::ProxyResolverConfiguration> getProxyResolverConfiguration(const Poco::Util::AbstractConfiguration * proxy_resolver_config)
    {
        auto endpoint = Poco::URI(proxy_resolver_config->getString("endpoint"));
        auto proxy_scheme = proxy_resolver_config->getString("proxy_scheme");
        if (proxy_scheme != "http" && proxy_scheme != "https")
            throw Exception("Only HTTP/HTTPS schemas allowed in proxy resolver config: " + proxy_scheme, ErrorCodes::BAD_ARGUMENTS);
        auto proxy_port = proxy_resolver_config->getUInt("proxy_port");

        LOG_DEBUG(
            &Logger::get("DiskS3"), "Configured proxy resolver: " << endpoint.toString() << ", Scheme: " << proxy_scheme << ", Port: " << proxy_port);

        return std::make_shared<S3::ProxyResolverConfiguration>(endpoint, proxy_scheme, proxy_port);
    }

    std::shared_ptr<S3::ProxyListConfiguration> getProxyListConfiguration(const Poco::Util::AbstractConfiguration * proxy_config)
    {
        std::vector<String> keys;
        proxy_config->keys(keys);

        std::vector<Poco::URI> proxies;
        for (const auto & key : keys)
            if (startsWith(key, "uri"))
            {
                Poco::URI proxy_uri(proxy_config->getString(key));

                if (proxy_uri.getScheme() != "http" && proxy_uri.getScheme() != "https")
                    throw Exception("Only HTTP/HTTPS schemas allowed in proxy uri: " + proxy_uri.toString(), ErrorCodes::BAD_ARGUMENTS);
                if (proxy_uri.getHost().empty())
                    throw Exception("Empty host in proxy uri: " + proxy_uri.toString(), ErrorCodes::BAD_ARGUMENTS);

                proxies.push_back(proxy_uri);

                LOG_DEBUG(&Logger::get("DiskS3"), "Configured proxy: " << proxy_uri.toString());
            }

        if (!proxies.empty())
            return std::make_shared<S3::ProxyListConfiguration>(proxies);

        return nullptr;
    }

    std::shared_ptr<S3::ProxyConfiguration> getProxyConfiguration(const Poco::Util::AbstractConfiguration * config)
    {
        if (!config->has("proxy"))
            return nullptr;

        const auto * proxy_config = config->createView("proxy");

        std::vector<String> config_keys;
        proxy_config->keys(config_keys);

        if (auto resolver_configs = std::count(config_keys.begin(), config_keys.end(), "resolver"))
        {
            if (resolver_configs > 1)
                throw Exception("Multiple proxy resolver configurations aren't allowed", ErrorCodes::BAD_ARGUMENTS);

            return getProxyResolverConfiguration(proxy_config->createView("resolver"));
        }

        return getProxyListConfiguration(proxy_config);
    }
}


void registerDiskS3(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      const Context & context) -> DiskPtr {
        const auto * disk_config = config.createView(config_prefix);

        Poco::File disk{context.getPath() + "disks/" + name};
        disk.createDirectories();

        Aws::Client::ClientConfiguration cfg;

        S3::URI uri(Poco::URI(disk_config->getString("endpoint")));
        if (uri.key.back() != '/')
            throw Exception("S3 path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

        cfg.endpointOverride = uri.endpoint;

        auto proxy_config = getProxyConfiguration(disk_config);
        if (proxy_config)
            cfg.perRequestConfiguration = [proxy_config](const auto & request) { return proxy_config->getConfiguration(request); };

        auto client = S3::ClientFactory::instance().create(
            cfg,
            disk_config->getString("access_key_id", ""),
            disk_config->getString("secret_access_key", ""));

        String metadata_path = context.getPath() + "disks/" + name + "/";

        auto s3disk = std::make_shared<DiskS3>(
            name,
            client,
            proxy_config,
            uri.bucket,
            uri.key,
            metadata_path,
            context.getSettingsRef().s3_min_upload_part_size);

        /// This code is used only to check access to the corresponding disk.
        checkWriteAccess(*s3disk);
        checkReadAccess(name, *s3disk);
        checkRemoveAccess(*s3disk);

        return s3disk;
    };
    factory.registerDiskType("s3", creator);
}

}
