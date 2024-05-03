#pragma once

#include <Poco/Net/HTTPClientSession.h>
#include <Common/ProxyConfiguration.h>

namespace DB
{

Poco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB::ProxyConfiguration & proxy_configuration);

}
