#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include "ProxyConfiguration.h"

namespace DB::S3
{
/**
 * Proxy configuration where proxy host is obtained each time from specified endpoint.
 * For each request to S3 it makes GET request to specified endpoint URL and reads proxy host from a response body.
 * Specified scheme and port added to obtained proxy host to form completed proxy URL.
 */
class ProxyResolverConfiguration : public ProxyConfiguration
{
public:
    ProxyResolverConfiguration(const Poco::URI & endpoint_, String proxy_scheme_, unsigned proxy_port_);
    Aws::Client::ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) override;

private:
    /// Endpoint to obtain a proxy host.
    const Poco::URI endpoint;
    /// Scheme for obtained proxy.
    const String proxy_scheme;
    /// Port for obtained proxy.
    const unsigned proxy_port;
};

}

#endif
