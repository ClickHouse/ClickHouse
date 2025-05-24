#pragma once

#include <Server/TCPProtocolStackData.h>
#include <Server/TCPServerConnectionFactory.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/logger_useful.h>

#include <ProxyServer/IProxyServer.h>
#include <ProxyServer/TCPHandler.h>

namespace Poco
{
class Logger;
}

namespace Proxy
{

class TCPHandlerFactory : public DB::TCPServerConnectionFactory
{
private:
    IProxyServer & server;
    bool parse_proxy_protocol = false;
    LoggerPtr log;
    std::string host_name;
    std::string server_display_name;

    RouterPtr router;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override { }
    };

public:
    TCPHandlerFactory(IProxyServer & server_, bool secure_, bool parse_proxy_protocol_, RouterPtr router_)
        : server(server_)
        , parse_proxy_protocol(parse_proxy_protocol_)
        , log(getLogger(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        , router(router_)
    {
        host_name = getFQDNOrHostName();
        server_display_name = server.config().getString("display_name", host_name);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, DB::TCPServer & tcp_server) override
    {
        try
        {
            LOG_INFO(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPHandler(server, tcp_server, socket, parse_proxy_protocol, router);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
