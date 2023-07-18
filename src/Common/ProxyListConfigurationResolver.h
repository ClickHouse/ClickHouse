#pragma once

#include <base/types.h>

#include <Common/ProxyConfigurationResolver.h>
#include <Common/AtomicRoundRobin.h>
#include <Poco/URI.h>

namespace DB
{

/*
 * Round-robin proxy list resolver.
 * */
class ProxyListConfigurationResolver : public ProxyConfigurationResolver
{
public:
    // I guess extra copy is happening here.
    ProxyListConfigurationResolver(std::vector<Poco::URI> http_proxies_, std::vector<Poco::URI> https_proxies_);

    ProxyConfiguration resolve(Method method) override;

    void errorReport(const ProxyConfiguration &) override {}

private:
    AtomicRoundRobin<Poco::URI> http_proxies;
    AtomicRoundRobin<Poco::URI> https_proxies;
    AtomicRoundRobin<Poco::URI> any_proxies;

    Poco::URI getProxyURI(Method method);
};

}
