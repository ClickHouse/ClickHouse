#pragma once

#include <Server/ProxyProtocolHandler.h>
#include <Server/HTTPProxyConfig.h>

namespace DB
{

class HTTPProxyProtocolHandler final : public HTTPServerRequestAwareProxyProtocolHandler
{
public:
    explicit HTTPProxyProtocolHandler(const HTTPProxyConfig & config_);

    virtual void handle(const HTTPServerRequest & request) override;

private:
    HTTPProxyConfig config;
};

}
