#pragma once

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/TCPServerConnection.h>

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>

namespace Poco { class Logger; }

namespace DB
{

class HTTPServerRequest;
class ProxyProtocolHandler;

class ProxyConfig;
using ProxyConfigs = std::map<std::string, std::unique_ptr<ProxyConfig>>;

class IndirectTCPServerConnection : public Poco::Net::TCPServerConnection
{
public:
    explicit IndirectTCPServerConnection(
        const std::string & interface_name,
        const Poco::Net::StreamSocket & socket,
        const ProxyConfigs & proxies,
        const std::set<std::string> & supported_proxy_protocols
    );

    void handleProxyProtocol(Poco::Net::StreamSocket & socket);
    void handleProxyProtocol(const HTTPServerRequest & request);

    bool isIndirect() const;

    const Poco::Net::IPAddress & initiatorPeerAddress() const;
    const Poco::Net::IPAddress & immediatePeerAddress() const;

private:
    Poco::Logger * log;
    const Poco::Net::IPAddress immediatePeer;
    std::unique_ptr<ProxyProtocolHandler> proxy_handler;
};

}
