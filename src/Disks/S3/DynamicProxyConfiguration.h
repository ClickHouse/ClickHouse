#pragma once

#include <utility>
#include <Core/Types.h>
#include <aws/core/client/ClientConfiguration.h>
#include <Poco/URI.h>

namespace DB::S3
{
class DynamicProxyConfiguration
{
public:
    explicit DynamicProxyConfiguration(std::vector<Poco::URI> _proxies);
    /// Returns proxy configuration on each HTTP request.
    Aws::Client::ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request);

private:
    /// List of configured proxies.
    const std::vector<Poco::URI> proxies;
    /// Access counter to get proxy using round-robin strategy.
    std::atomic<size_t> access_counter;
};

}
