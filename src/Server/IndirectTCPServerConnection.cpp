#include <Server/IndirectTCPServerConnection.h>
#include <Server/ProxyConfig.h>
#include <Server/ProxyProtocolHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <common/logger_useful.h>

#include <Poco/Logger.h>

#include <boost/asio/ip/address.hpp>

namespace DB
{

namespace
{

template <typename Address, typename Network>
bool addrInNet(const Address &, const Network &)
{
    return false;
}

bool addrInNet(const boost::asio::ip::address_v4 & address, const boost::asio::ip::network_v4 & network)
{
    const auto range = network.hosts();
    return range.find(address) != range.end();
}

bool addrInNet(const boost::asio::ip::address_v6 & address, const boost::asio::ip::network_v6 & network)
{
    const auto range = network.hosts();
    return range.find(address) != range.end();
}

bool addrInNet(const boost::asio::ip::address & address, const ProxyConfig::Network & network)
{
    return std::visit(
        [&] (const auto & net)
        {
            return (address.is_v4() ? addrInNet(address.to_v4(), net) : addrInNet(address.to_v6(), net));
        },
        network
    );
}

bool addrInNet(const boost::asio::ip::address & address, const ProxyConfig::Networks & networks)
{
    for (const auto & network : networks)
    {
        if (addrInNet(address, network))
            return true;
    }
    return false;
}

}

IndirectTCPServerConnection::IndirectTCPServerConnection(
    const std::string & interface_name,
    const Poco::Net::StreamSocket & socket,
    const ProxyConfigs & proxies,
    const std::set<std::string> & supported_proxy_protocols
)
    : Poco::Net::TCPServerConnection(socket)
    , log(&Poco::Logger::get("Interface: " + interface_name))
    , immediatePeer(socket.peerAddress().host())
{
    const auto address = boost::asio::ip::make_address(immediatePeer.toString());
    for (const auto & proxy : proxies)
    {
        const auto & proxy_config = *proxy.second;
        if (addrInNet(address, proxy_config.trusted_networks))
        {
            if (supported_proxy_protocols.count(proxy_config.protocol) == 0)
            {
                LOG_WARNING(log, "Proxy protocol " + proxy_config.protocol + " is not supported for the interface, ignoring...");
            }
            else if (proxy_handler)
            {
                LOG_WARNING(log, "More than one proxy is configured to match the peer IP, using the first one...");
            }
            else
            {
                proxy_handler = proxy_config.createProxyProtocolHandler();
            }
        }
    }
}

void IndirectTCPServerConnection::handleProxyProtocol(Poco::Net::StreamSocket & socket)
{
    if (auto * stream_proxy_handler = dynamic_cast<StreamSocketAwareProxyProtocolHandler *>(proxy_handler.get()))
        stream_proxy_handler->handle(socket);
}

void IndirectTCPServerConnection::handleProxyProtocol(const HTTPServerRequest & request)
{
    if (auto * http_proxy_handler = dynamic_cast<HTTPServerRequestAwareProxyProtocolHandler *>(proxy_handler.get()))
        http_proxy_handler->handle(request);
}

bool IndirectTCPServerConnection::isIndirect() const
{
    return (proxy_handler && !proxy_handler->peerAddressChain().empty());
}

const Poco::Net::IPAddress & IndirectTCPServerConnection::initiatorPeerAddress() const
{
    return (isIndirect() ? proxy_handler->peerAddressChain().front() : immediatePeerAddress());
}

const Poco::Net::IPAddress & IndirectTCPServerConnection::immediatePeerAddress() const
{
    return immediatePeer;
}

}
