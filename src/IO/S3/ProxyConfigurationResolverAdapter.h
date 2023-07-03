#pragma once

#include <IO/S3/ProxyConfiguration.h>
#include <Common/ProxyConfigurationResolver.h>

#if USE_AWS_S3

namespace DB::S3
{

/*
 * Common/ProxyConfigurationResolver has been built to serve as a generic Proxy Configuration Resolver as an attempt to re-use it
 * in other parts of the project (e.g, URL functions). Therefore, it defines its own API and data types. To keep this backwards compatible
 * and simplify usage on S3 existing APIs, this adapter is used to convert from Common/ProxyConfiguration to ClientConfigurationPerRequest.
 * */
class ProxyConfigurationResolverAdapter : public ProxyConfiguration
{
public:
    explicit ProxyConfigurationResolverAdapter(std::shared_ptr<ProxyConfigurationResolver> resolver_)
        : resolver(resolver_) {}
    /// Returns proxy configuration on each HTTP request.
    ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) override;
    void errorReport(const ClientConfigurationPerRequest & config) override;

private:
    std::shared_ptr<ProxyConfigurationResolver> resolver;
};

}

#endif
