#pragma once

#include <Server/ProxyProtocolHandler.h>
#include <Server/PROXYProxyConfig.h>

namespace DB
{

class PROXYProxyProtocolHandler final
    : public TCPProxyProtocolHandler
    , public HTTPProxyProtocolHandler
{
public:
    explicit PROXYProxyProtocolHandler(const PROXYProxyConfig & config_);

    virtual void handle(const Poco::Net::StreamSocket & socket) override;
    virtual void handle(const HTTPServerRequest & request) override;

private:
    static std::optional<Poco::Net::IPAddress> consumePROXYv1Header(const Poco::Net::StreamSocket & socket);
    static std::optional<Poco::Net::IPAddress> consumePROXYv2Header(const Poco::Net::StreamSocket & socket);

private:
    PROXYProxyConfig config;
    std::optional<Poco::Net::IPAddress> prevStreamParsedPeer;
};

}
