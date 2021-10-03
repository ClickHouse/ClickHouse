#include <Server/IndirectServerConnection.h>
#include <Server/ProxyConfig.h>
#include <Server/ProxyProtocolHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <base/logger_useful.h>

#include <Poco/Logger.h>

#include <boost/asio/ip/address.hpp>

namespace DB
{

namespace
{

template <typename ProxyProtocolHandlerInterface>
std::unique_ptr<ProxyProtocolHandlerInterface> makeProxyHandler(
    const std::string & interface_name,
    const Poco::Net::StreamSocket & socket,
    const ProxyConfigs & proxies
)
{
    std::unique_ptr<ProxyProtocolHandlerInterface> proxy_handler;

    const auto * log = &Poco::Logger::get("Interface: " + interface_name);
    const auto peer_address = boost::asio::ip::make_address(socket.peerAddress().host().toString());

    for (const auto & proxy : proxies)
    {
        const auto & proxy_config = *proxy.second;
        if (Util::addrInNet(peer_address, proxy_config.trusted_networks))
        {
            auto tmp_proxy_handler = proxy_config.createProxyProtocolHandler();
            if (auto * tmp_specific_proxy_handler = dynamic_cast<ProxyProtocolHandlerInterface *>(tmp_proxy_handler.get()))
            {
                if (proxy_handler)
                    LOG_WARNING(log, "More than one compatible proxy is configured to match the peer IP, using the first one...");
                else
                {
                    tmp_proxy_handler.release();
                    proxy_handler.reset(tmp_specific_proxy_handler);
                }
            }
        }
    }

    return proxy_handler;
}

}

IndirectTCPServerConnection::IndirectTCPServerConnection(
    const std::string & interface_name,
    const Poco::Net::StreamSocket & socket,
    const ProxyConfigs & proxies
)
    : Poco::Net::TCPServerConnection(socket)
    , tcp_proxy_handler(makeProxyHandler<StreamSocketAwareProxyProtocolHandler>(interface_name, socket, proxies))
{
}

void IndirectTCPServerConnection::handleProxyProtocol(Poco::Net::StreamSocket & socket)
{
    if (tcp_proxy_handler)
        tcp_proxy_handler->handle(socket);
}

bool IndirectTCPServerConnection::isIndirect() const
{
    return !getAddressChain().empty();
}

const std::vector<Poco::Net::IPAddress> & IndirectTCPServerConnection::getAddressChain() const
{
    if (tcp_proxy_handler)
        return tcp_proxy_handler->getAddressChain();

    static const std::vector<Poco::Net::IPAddress> empty_address_chain;
    return empty_address_chain;
}

IndirectHTTPServerConnection::IndirectHTTPServerConnection(
    const std::string & interface_name,
    const Poco::Net::StreamSocket & socket,
    const ProxyConfigs & proxies
)
    : IndirectTCPServerConnection(interface_name, socket, proxies)
    , http_proxy_handler(makeProxyHandler<HTTPServerRequestAwareProxyProtocolHandler>(interface_name, socket, proxies))
{
}

void IndirectHTTPServerConnection::handleProxyProtocol(const HTTPServerRequest & request)
{
    if (http_proxy_handler)
        http_proxy_handler->handle(request);
}

bool IndirectHTTPServerConnection::isIndirect() const
{
    return !getAddressChain().empty();
}

const std::vector<Poco::Net::IPAddress> & IndirectHTTPServerConnection::getAddressChain() const
{
    // If there are addresses extracted from the raw TCP stream, prefer them over the addresses extracted from HTTP request.
    if (!IndirectTCPServerConnection::isIndirect() && http_proxy_handler)
        return http_proxy_handler->getAddressChain();

    return IndirectTCPServerConnection::getAddressChain();
}

}
