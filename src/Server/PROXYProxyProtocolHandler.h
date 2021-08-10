#pragma once

#include <Server/ProxyProtocolHandler.h>
#include <Server/PROXYProxyConfig.h>

namespace DB
{

class PROXYProxyProtocolHandler final : public StreamSocketAwareProxyProtocolHandler
{
public:
    explicit PROXYProxyProtocolHandler(const PROXYProxyConfig & config_);

    virtual void handle(Poco::Net::StreamSocket & socket) override;

private:
    PROXYProxyConfig config;
};

}
