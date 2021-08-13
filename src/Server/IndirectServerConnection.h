#pragma once

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/TCPServerConnection.h>

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace Poco { class Logger; }

namespace DB
{

class HTTPServerRequest;
class StreamSocketAwareProxyProtocolHandler;
class HTTPServerRequestAwareProxyProtocolHandler;

class ProxyConfig;
using ProxyConfigs = std::map<std::string, std::unique_ptr<ProxyConfig>>;

class IndirectTCPServerConnection : public Poco::Net::TCPServerConnection
{
public:
    explicit IndirectTCPServerConnection(
        const std::string & interface_name,
        const Poco::Net::StreamSocket & socket,
        const ProxyConfigs & proxies
    );

    void handleProxyProtocol(Poco::Net::StreamSocket & socket);

    bool isIndirect() const;
    const std::vector<Poco::Net::IPAddress> & getAddressChain() const;

private:
    std::unique_ptr<StreamSocketAwareProxyProtocolHandler> tcp_proxy_handler;
};

class IndirectHTTPServerConnection : public IndirectTCPServerConnection
{
public:
    explicit IndirectHTTPServerConnection(
        const std::string & interface_name,
        const Poco::Net::StreamSocket & socket,
        const ProxyConfigs & proxies
    );

    using IndirectTCPServerConnection::handleProxyProtocol;
    void handleProxyProtocol(const HTTPServerRequest & request);

    bool isIndirect() const;
    const std::vector<Poco::Net::IPAddress> & getAddressChain() const;

private:
    std::unique_ptr<HTTPServerRequestAwareProxyProtocolHandler> http_proxy_handler;
};

}
