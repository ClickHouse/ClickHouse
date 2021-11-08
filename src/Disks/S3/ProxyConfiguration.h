#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <utility>
#include <base/types.h>
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
    virtual void errorReport(const Aws::Client::ClientConfigurationPerRequest & config) = 0;
};

}

#endif
