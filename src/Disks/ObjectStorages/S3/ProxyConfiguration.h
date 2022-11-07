#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <utility>
#include <base/types.h>
#include <IO/S3/PocoHTTPClient.h>
#include <Poco/URI.h>

namespace DB::S3
{
class ProxyConfiguration
{
public:
    virtual ~ProxyConfiguration() = default;
    /// Returns proxy configuration on each HTTP request.
    virtual ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) = 0;
    virtual void errorReport(const ClientConfigurationPerRequest & config) = 0;
};

}

#endif
