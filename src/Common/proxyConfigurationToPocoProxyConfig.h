#pragma once

#include <Poco/Net/HTTPClientSession.h>
#include <Common/ProxyConfiguration.h>

namespace DB
{

Poco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB::ProxyConfiguration & proxy_configuration);

std::string buildPocoNonProxyHosts(const std::string & no_proxy_hosts_string);

}
