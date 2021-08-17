#include <Server/HTTPProxyConfig.h>
#include <Server/HTTPProxyProtocolHandler.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

HTTPProxyConfig::HTTPProxyConfig(const std::string & name_)
    : ProxyConfig(name_, "HTTP")
{
}

std::unique_ptr<ProxyConfig> HTTPProxyConfig::clone() const
{
    return std::make_unique<HTTPProxyConfig>(*this);
}

void HTTPProxyConfig::updateConfig(const Poco::Util::AbstractConfiguration & config)
{
    ProxyConfig::updateConfig(config);

    if (config.has("proxy_chain_limit"))
        proxy_chain_limit = config.getUInt64("proxy_chain_limit");
}

std::unique_ptr<ProxyProtocolHandler> HTTPProxyConfig::createProxyProtocolHandler() const
{
    return std::make_unique<HTTPProxyProtocolHandler>(*this);
}

}
