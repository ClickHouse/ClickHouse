#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <atomic> // for std::atomic<size_t>

#include "ProxyConfiguration.h"

namespace DB::S3
{
/**
 * For each request to S3 it chooses a proxy from the specified list using round-robin strategy.
 */
class ProxyListConfiguration : public ProxyConfiguration
{
public:
    explicit ProxyListConfiguration(std::vector<Poco::URI> proxies_);
    ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) override;
    void errorReport(const ClientConfigurationPerRequest &) override {}

private:
    /// List of configured proxies.
    const std::vector<Poco::URI> proxies;
    /// Access counter to get proxy using round-robin strategy.
    std::atomic<size_t> access_counter;
};

}

#endif
