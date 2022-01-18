#pragma once

#include <utility>
#include <common/types.h>
#include <aws/core/client/ClientConfiguration.h>
#include <Poco/URI.h>

namespace DB::S3
{
class ProxyConfiguration
{
public:
    virtual ~ProxyConfiguration() = default;
    /// Returns proxy configuration on each HTTP request.
    virtual Aws::Client::ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) = 0;
};

}
